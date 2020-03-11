package ink.anur.engine.log.fetch

import ink.anur.core.raft.ElectionMetaService
import ink.anur.debug.Debugger
import ink.anur.engine.log.prelog.ByteBufPreLogService
import ink.anur.inject.Event
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.inject.NigateListener
import ink.anur.pojo.server.FetchResponse

/**
 * Created by Anur IjuoKaruKas on 2020/3/11
 *
 * follower 负责不断从 leader fetch 消息
 */
@NigateBean
class FollowerFetcherService : LogFetcher() {

    private val logger = Debugger(this.javaClass)

    @NigateInject
    private lateinit var electionMetaService: ElectionMetaService

    @NigateInject
    private lateinit var byteBufPreLogService: ByteBufPreLogService

    override fun howToConsumeFetchResponse(fetchFrom: String, fetchResponse: FetchResponse) {
        logger.trace("收到节点 {} 返回的 FETCH_RESPONSE", fetchFrom)

        if (electionMetaService.isLeader()) {
            logger.error("出现了不应该出现的情况！是 follower 却收到了 leader 该收到的消息")
        } else if (fetchResponse.size() != 0) {
            byteBufPreLogService.append(fetchResponse.generation, fetchResponse.read())
        }
    }

    @NigateListener(onEvent = Event.RECOVERY_COMPLETE)
    private fun whileRecoveryComplete() {
        startToFetchFromLeader()
    }

    @NigateListener(onEvent = Event.COORDINATE_DISCONNECT_FROM_LEADER)
    private fun whileDisconnectFromLeader() {
        cancelFetchTask()
    }

    @NigateListener(onEvent = Event.CLUSTER_INVALID)
    private fun whileClusterInvalid() {
        writeLocker {
            cancelFetchTask()

//             当集群不可用时，与协调 leader 断开连接 todo
//            CoordinateClientOperator.shutDownInstance("集群已不可用，与协调 Leader 断开连接")
        }
    }
}