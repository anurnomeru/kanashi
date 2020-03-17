package ink.anur.service.log

import ink.anur.core.common.AbstractRequestMapping
import ink.anur.core.common.RequestExtProcessor
import ink.anur.core.raft.ElectionMetaService
import ink.anur.core.request.RequestProcessCentreService
import ink.anur.debug.Debugger
import ink.anur.engine.log.LeaderLogConsistenceService
import ink.anur.engine.log.common.LogService
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.pojo.enumerate.RequestTypeEnum
import ink.anur.pojo.log.Commit
import ink.anur.pojo.log.CommitResponse
import ink.anur.pojo.server.Fetch
import ink.anur.pojo.server.FetchResponse
import io.netty.channel.Channel
import java.nio.ByteBuffer

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
    private lateinit var leaderLogConsistenceService: LeaderLogConsistenceService

    override fun typeSupport(): RequestTypeEnum {
        return RequestTypeEnum.FETCH
    }

    override fun handleRequest(fromServer: String, msg: ByteBuffer, channel: Channel) {
        val fetcher = Fetch(msg)

        // 所以如果不是集群恢复阶段, 当leader收到 fetchRequest, 需要发送 COMMIT 类型的消息, 内容为当前 canCommit 的 GAO
        // 同时, 在集群成员收到COMMIT 消息时,需要回复一个 COMMIT RESPONSE,表明自己的 fetch 进度
        if (electionMetaService.isLeader()) {
            val canCommit = leaderLogConsistenceService.fetchReport(fromServer, fetcher.fetchGAO)

            requestProcessCentreService.send(fromServer, Commit(canCommit), RequestExtProcessor(
                { byteBuffer ->
                    val commitResponse = CommitResponse(byteBuffer)
                    leaderLogConsistenceService.commitReport(fromServer, commitResponse.commitGAO)
                }))
        }

        // 为什么要。next，因为 fetch 过来的是客户端最新的 GAO 进度，而获取的要从 GAO + 1开始
        val fetchDataInfo = logService.getAfter(fetcher.fetchGAO.next())
        if (fetchDataInfo == null) logger.debug("对于 fetch 请求 ${fetcher.fetchGAO}， 返回为空")

        requestProcessCentreService.send(fromServer, FetchResponse(fetchDataInfo))
    }
}