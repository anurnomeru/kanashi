package ink.anur.service

import ink.anur.struct.enumerate.OperationTypeEnum
import ink.anur.core.coordinator.common.AbstractRequestMapping
import ink.anur.core.raft.RaftCenterController
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.struct.HeartBeat
import io.netty.channel.Channel
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2020/2/28
 *
 * 专门处理心跳的处理器
 */
@NigateBean
class HeartbeatHandleService : AbstractRequestMapping() {

    @NigateInject
    private lateinit var raftCenterController: RaftCenterController

    override fun typeSupport(): OperationTypeEnum {
        return OperationTypeEnum.HEAT_BEAT
    }

    override fun handleRequest(fromServer: String, msg: ByteBuffer, channel: Channel) {
        val heartbeat = HeartBeat(msg)
        raftCenterController.receiveHeatBeat(fromServer, heartbeat.generation)
    }
}