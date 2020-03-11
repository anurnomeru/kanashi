package ink.anur.engine.log

import ink.anur.core.raft.ElectionMetaService
import ink.anur.debug.Debugger
import ink.anur.engine.StoreEngineFacadeService
import ink.anur.engine.log.prelog.ByteBufPreLogService
import ink.anur.engine.persistence.CommitProcessManageService
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.mutex.ReentrantReadWriteLocker
import ink.anur.pojo.log.common.GenerationAndOffset
import java.util.concurrent.ConcurrentSkipListMap

/**
 * Created by Anur IjuoKaruKas on 2020/3/11
 *
 * leader 的日志一次性控制器
 */
@NigateBean
class LeaderLogConsistenceService : ReentrantReadWriteLocker() {

    private val logger = Debugger(this.javaClass)

    @NigateInject
    private lateinit var commitProcessManageService: CommitProcessManageService

    @NigateInject
    private lateinit var electMetaService: ElectionMetaService

    @NigateInject
    private lateinit var byteBufPreLogService: ByteBufPreLogService

    @NigateInject
    private lateinit var storeEngineFacadeService: StoreEngineFacadeService

    /**
     * 作为 Leader 时有效，维护了每个节点的 fetch 进度
     */
    @Volatile
    private var fetchMap = ConcurrentSkipListMap<GenerationAndOffset, MutableSet<String>>()

    /**
     * 作为 Leader 时有效，记录了每个节点最近的一次 fetch
     */
    @Volatile
    private var currentFetchMap = mutableMapOf<String, GenerationAndOffset>()

    /**
     * 作为 Leader 时有效，维护了每个节点的 commit 进度
     */
    @Volatile
    private var commitMap = ConcurrentSkipListMap<GenerationAndOffset, MutableSet<String>>()

    /**
     * 作为 Leader 时有效，记录了每个节点最近的一次 commit
     */
    @Volatile
    private var currentCommitMap = mutableMapOf<String, GenerationAndOffset>()

    /**
     * Follower 向 Leader 提交拉取到的最大的 GAO
     *
     * 如果某个最大的 GAO 已经达到了 commit 条件，将返回此 GAO。
     */
    fun fetchReport(node: String, GAO: GenerationAndOffset): GenerationAndOffset {
        val latestGAO = commitProcessManageService.load()

        if (!electMetaService.isLeader()) {
            return latestGAO
        }

        if (latestGAO > GAO) {// 小于已经 commit 的 GAO 无需记录
            return latestGAO
        }

        val currentGAO = readLockSupplier { currentFetchMap[node] }

        return if (currentGAO != null && currentGAO >= GAO) {// 小于之前提交记录的无需记录
            latestGAO
        } else {
            writeLockSupplierCompel {
                // 移除之前的 fetch 记录
                currentGAO?.also {
                    logger.debug("节点 {} fetch 进度由 {} 更新到了进度 {}", node, it.toString(), GAO.toString())
                    fetchMap[it]!!.remove(node)
                } ?: logger.debug("节点 {} 已经 fetch 更新到了进度 {}", node, GAO.toString())

                currentFetchMap[node] = GAO// 更新节点的 fetch 进度
                fetchMap.compute(GAO) { // 更新节点最近一次 fetch 处于哪个 GAO
                    _, strings ->
                    (strings ?: mutableSetOf()).also { it.add(node) }
                }

                // 找到最大的那个票数 >= quorum 的 fetch GAO
                fetchMap.entries.findLast { e -> e.value.size + 1 >= electMetaService.quorum }?.key?.also { logger.debug("进度 {} 已可提交 ~ 已经拟定 approach，半数节点同意则进行 commit", it.toString()) }
                    ?: latestGAO
            }
        }
    }

    /**
     * leader 收到来自客户端的 commitReport,并且 cover 自身进度
     */
    fun commitReport(node: String, commitGAO: GenerationAndOffset) {
        val latestCommitGAO = commitProcessManageService.load()

        if (!electMetaService.isLeader()) {
            logger.error("不是leader不太可能收到 commitReport！ 很可能是有BUG ")
            return
        }

        if (latestCommitGAO > commitGAO) {// 小于已经 commit 的 GAO 直接无视
            return
        }

        val currentCommitGAO = readLockSupplier { currentCommitMap[node] }
        if (currentCommitGAO != null && currentCommitGAO >= commitGAO) {// 小于之前提交记录的无需记录
            return
        }

        writeLockSupplierCompel {

            /*
             * 1、移除节点旧的 commit 进度
             */
            currentCommitGAO?.also {
                logger.debug("节点 {} 的 commit 进度由 {} 更新到了进度 {}", node, it.toString(), commitGAO.toString())
                commitMap[it]!!.remove(node)
            } ?: logger.debug("节点 {} 的 commit 更新到了进度 {}", node, commitGAO.toString())

            /*
             * 2、移除节点旧的 commit进度，并记录最新的一次 commit 进度
             *
             * 记录在 currentCommitMap 记录一次
             *    在 commitMap 记录一次
             */
            currentCommitMap[node] = commitGAO//更新节点的 commit 进度
            commitMap.compute(commitGAO) { // 更新节点最近一次 commit 处于哪个 GAO
                _, strings ->
                (strings ?: mutableSetOf()).also {
                    it.add(node)
                }
            }

            /*
             * 3、找到最大的那个票数 >= quorum 的 commit GAO
             *
             * 将最高记录写入本地
             */
            commitMap.entries.findLast { e -> e.value.size + 1 >= electMetaService.quorum }
                ?.key
                ?.also {
                    // 写入 ByteBufPreLogManager(避免成为follower没有这个进度)
                    byteBufPreLogService.cover(it)
                    // 写入本地文件，并通知存储引擎继续工作
                    storeEngineFacadeService.coverCommittedProjectGenerationAndOffset(it)
                    logger.debug("进度 {} 已经完成 commit ~", it.toString())
                }
                ?: latestCommitGAO
        }
    }
}