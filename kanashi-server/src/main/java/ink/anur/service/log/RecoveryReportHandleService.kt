package ink.anur.service.log

import ink.anur.core.common.AbstractRequestMapping
import ink.anur.core.raft.ElectionMetaService
import ink.anur.debug.Debugger
import ink.anur.engine.log.recovery.LeaderClusterRecoveryService
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.pojo.enumerate.RequestTypeEnum
import ink.anur.pojo.log.RecoveryReporter
import io.netty.channel.Channel
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2020/3/12
 *
 * 处理 recovery report
 */
@NigateBean
class RecoveryReportHandleService : AbstractRequestMapping() {

    private val logger = Debugger(this::class.java)

    @NigateInject
    private lateinit var electionMetaService: ElectionMetaService

    @NigateInject
    private lateinit var leaderClusterRecoveryService: LeaderClusterRecoveryService

    override fun typeSupport(): RequestTypeEnum {
        return RequestTypeEnum.RECOVERY_REPORTER
    }

    override fun handleRequest(fromServer: String, msg: ByteBuffer, channel: Channel) {
        val recoveryReporter = RecoveryReporter(msg)
        if (!electionMetaService.isLeader()) {
            logger.error("不是leader却收到了 RecoveryReport !??")
        } else {
            leaderClusterRecoveryService.receive(fromServer, recoveryReporter.getCommited())
        }
    }
}