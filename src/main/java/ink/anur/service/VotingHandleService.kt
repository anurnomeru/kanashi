package ink.anur.service

import ink.anur.struct.enumerate.OperationTypeEnum
import ink.anur.core.coordinator.common.AbstractRequestMapping
import ink.anur.core.raft.RaftCenterController
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.struct.Canvass
import ink.anur.struct.Voting
import io.netty.channel.Channel
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2020/2/27
 *
 * 专门处理拉票的处理器
 */
@NigateBean
class VotingHandleService : AbstractRequestMapping() {

    @NigateInject
    private lateinit var raftCenterController: RaftCenterController

    override fun typeSupport(): OperationTypeEnum {
        return OperationTypeEnum.VOTING
    }

    override fun handleRequest(fromServer: String, msg: ByteBuffer, channel: Channel) {
        val voting = Voting(msg)
        raftCenterController.receiveVote(fromServer, voting)
    }
}