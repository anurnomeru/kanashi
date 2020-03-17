package ink.anur.service.log

import ink.anur.core.common.AbstractRequestMapping
import ink.anur.core.raft.ElectionMetaService
import ink.anur.debug.Debugger
import ink.anur.engine.StoreEngineFacadeService
import ink.anur.engine.log.prelog.ByteBufPreLogService
import ink.anur.engine.persistence.CommitProcessManageService
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.mutex.ReentrantReadWriteLocker
import ink.anur.pojo.enumerate.RequestTypeEnum
import ink.anur.pojo.log.CommitResponse
import ink.anur.pojo.log.common.GenerationAndOffset
import io.netty.channel.Channel
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentSkipListMap

/**
 * Created by Anur IjuoKaruKas on 2020/3/17
 *
 * 在集群成员收到COMMIT 消息时,需要回复一个 COMMIT RESPONSE,表明自己的 fetch 进度
 */
@NigateBean
class CommitResponseHandlerService : AbstractRequestMapping() {

    private var logger = Debugger(this.javaClass)

    @NigateInject
    private lateinit var commitProcessManageService: CommitProcessManageService

    @NigateInject
    private lateinit var electionMetaService: ElectionMetaService

    @NigateInject
    private lateinit var byteBufPreLogService: ByteBufPreLogService

    @NigateInject
    private lateinit var storeEngineFacadeService: StoreEngineFacadeService

    private val locker = ReentrantReadWriteLocker()

    /**
     * 作为 Leader 时有效，记录了每个节点最近的一次 commit
     */
    @Volatile
    private var currentCommitMap = mutableMapOf<String, GenerationAndOffset>()

    /**
     * 作为 Leader 时有效，维护了每个节点的 commit 进度
     */
    @Volatile
    private var commitMap = ConcurrentSkipListMap<GenerationAndOffset, MutableSet<String>>()

    override fun typeSupport(): RequestTypeEnum {
        return RequestTypeEnum.COMMIT_RESPONSE
    }

    override fun handleRequest(fromServer: String, msg: ByteBuffer, channel: Channel) {
        val commitGAO = CommitResponse(msg).commitGAO

        val latestCommitGAO = commitProcessManageService.load()

        if (!electionMetaService.isLeader()) {
            logger.error("不是leader不太可能收到 commitReport！ 很可能是有BUG ")
            return
        }

        if (latestCommitGAO > commitGAO) {// 小于已经 commit 的 GAO 直接无视
            return
        }

        val currentCommitGAO = locker.readLockSupplier { currentCommitMap[fromServer] }
        if (currentCommitGAO != null && currentCommitGAO >= commitGAO) {// 小于之前提交记录的无需记录
            return
        }

        locker.writeLockSupplierCompel {

            /*
             * 1、移除节点旧的 commit 进度
             */
            currentCommitGAO?.also {
                logger.debug("节点 {} 的 commit 进度由 {} 更新到了进度 {}", fromServer, it.toString(), commitGAO.toString())
                commitMap[it]!!.remove(fromServer)
            } ?: logger.debug("节点 {} 的 commit 更新到了进度 {}", fromServer, commitGAO.toString())

            /*
             * 2、移除节点旧的 commit进度，并记录最新的一次 commit 进度
             *
             * 记录在 currentCommitMap 记录一次
             *    在 commitMap 记录一次
             */
            currentCommitMap[fromServer] = commitGAO//更新节点的 commit 进度
            commitMap.compute(commitGAO) { // 更新节点最近一次 commit 处于哪个 GAO
                _, strings ->
                (strings ?: mutableSetOf()).also {
                    it.add(fromServer)
                }
            }

            /*
             * 3、找到最大的那个票数 >= quorum 的 commit GAO
             *
             * 将最高记录写入本地
             */
            commitMap.entries.findLast { e -> e.value.size + 1 >= electionMetaService.quorum }
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