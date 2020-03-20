package ink.anur.service.log

import ink.anur.core.common.AbstractRequestMapping
import ink.anur.core.raft.ElectionMetaService
import ink.anur.core.request.RequestProcessCentreService
import ink.anur.debug.Debugger
import ink.anur.engine.log.LogService
import ink.anur.engine.log.CommitProcessManageService
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.mutex.ReentrantReadWriteLocker
import ink.anur.pojo.enumerate.RequestTypeEnum
import ink.anur.pojo.log.Commit
import ink.anur.pojo.log.common.GenerationAndOffset
import ink.anur.pojo.server.Fetch
import ink.anur.pojo.server.FetchResponse
import io.netty.channel.Channel
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentSkipListMap

/**
 * Created by Anur IjuoKaruKas on 2020/3/12
 *
 * 向某个节点发送 fetch 消息，标明自己要 fetch 的 log 进度
 *
 * 节点会将这部分数据分批次返回回去（需要多次请求）
 *
 * 如果当前节点是主节点，还会根据情况返回当前可提交的 GAO (Commit)
 */
@NigateBean
class FetchHandlerService : AbstractRequestMapping() {

    private val logger = Debugger(this::class.java)

    @NigateInject
    private lateinit var electionMetaService: ElectionMetaService

    @NigateInject
    private lateinit var logService: LogService

    @NigateInject
    private lateinit var requestProcessCentreService: RequestProcessCentreService

    @NigateInject
    private lateinit var commitProcessManageService: CommitProcessManageService

    @NigateInject
    private lateinit var electMetaService: ElectionMetaService

    private val locker = ReentrantReadWriteLocker()

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

    override fun typeSupport(): RequestTypeEnum {
        return RequestTypeEnum.FETCH
    }

    override fun handleRequest(fromServer: String, msg: ByteBuffer, channel: Channel) {
        val fetcher = Fetch(msg)

        // 所以如果不是集群恢复阶段, 当leader收到 fetchRequest, 需要发送 COMMIT 类型的消息, 内容为当前 canCommit 的 GAO
        // 同时, 在集群成员收到COMMIT 消息时,需要回复一个 COMMIT RESPONSE,表明自己的 fetch 进度
        if (electionMetaService.isLeader()) {
            val fetcherGAO = fetcher.fetchGAO
            val latestGAO = commitProcessManageService.load()

            val canCommit = if (!electMetaService.isLeader()) {
                latestGAO
            } else {
                if (latestGAO > fetcherGAO) {// 小于已经 commit 的 GAO 无需记录
                    latestGAO
                } else {
                    val currentGAO = locker.readLockSupplier { currentFetchMap[fromServer] }

                    if (currentGAO != null && currentGAO >= fetcherGAO) {// 小于之前提交记录的无需记录
                        latestGAO
                    } else {
                        locker.writeLockSupplierCompel {
                            // 移除之前的 fetch 记录
                            currentGAO?.also {
                                logger.debug("节点 {} fetch 进度由 {} 更新到了进度 {}", fromServer, it.toString(), fromServer)
                                fetchMap[it]!!.remove(fromServer)
                            } ?: logger.debug("节点 {} 已经 fetch 更新到了进度 {}", fromServer, fetcherGAO)

                            currentFetchMap[fromServer] = fetcherGAO// 更新节点的 fetch 进度
                            fetchMap.compute(fetcherGAO) { // 更新节点最近一次 fetch 处于哪个 GAO
                                _, strings ->
                                (strings ?: mutableSetOf()).also { it.add(fromServer) }
                            }

                            // 找到最大的那个票数 >= quorum 的 fetch GAO
                            fetchMap.entries.findLast { e -> e.value.size + 1 >= electMetaService.quorum }?.key?.also { logger.debug("进度 {} 已可提交 ~ 已经拟定 approach，半数节点同意则进行 commit", it) }
                                ?: latestGAO
                        }
                    }
                }
            }

            requestProcessCentreService.send(fromServer, Commit(canCommit))
        }

        // 为什么要。next，因为 fetch 过来的是客户端最新的 GAO 进度，而获取的要从 GAO + 1开始
        val fetchDataInfo = logService.getAfter(fetcher.fetchGAO.next())
        if (fetchDataInfo != null) {
            requestProcessCentreService.send(fromServer, FetchResponse(fetchDataInfo))
        }
    }
}