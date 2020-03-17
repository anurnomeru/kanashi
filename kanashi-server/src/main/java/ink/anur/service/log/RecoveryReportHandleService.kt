package ink.anur.service.log

import ink.anur.core.common.AbstractRequestMapping
import ink.anur.core.raft.ElectionMetaService
import ink.anur.core.request.RequestProcessCentreService
import ink.anur.debug.Debugger
import ink.anur.engine.log.prelog.ByteBufPreLogService
import ink.anur.engine.log.recovery.ClusterRecoveryService
import ink.anur.inject.Event
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.inject.NigateListener
import ink.anur.pojo.enumerate.RequestTypeEnum
import ink.anur.pojo.log.RecoveryReporter
import io.netty.channel.Channel
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2020/3/12
 *
 * 当服务器挂机或者不正常时，需要进行集群日志的恢复
 *
 * 当选主成功后(cluster_valid)
 *
 * - 所有节点上报其最大 commit offset
 *
 * - 进行 recovery waiting n sec，直到所有节点上报数据
 *
 * -- 是否达到半数节点上报 => no => 节点一直阻塞，直到有半数节点上报
 *
 * |
 * |
 * V
 * yes
 *
 * - 获取最大的commit，作为 recovery point，最小的 commit 则作为 commit GAO
 *
 * -- leader 是否达到此 commit 数据 => no => 向拥有此数据的节点进行 fetch
 *
 * -- 下发指令，删除大于此 recovery point 的数据（针对前leader）
 *
 * |
 * |
 * V
 *
 * 集群可用
 *
 * //////////////////////////////////////////////////////////////////////////////////
 *
 * 集群可用后连上leader的需要做特殊处理：
 *
 * 需要检查当前世代 的last Offset，进行check，如果与leader不符，则需要truncate后恢复可用。
 */
@NigateBean
class RecoveryReportHandleService : AbstractRequestMapping() {

    private val logger = Debugger(this::class.java)

    @NigateInject
    private lateinit var electionMetaService: ElectionMetaService

    @NigateInject
    private lateinit var clusterRecoveryService: ClusterRecoveryService

    override fun typeSupport(): RequestTypeEnum {
        return RequestTypeEnum.RECOVERY_REPORTER
    }

    override fun handleRequest(fromServer: String, msg: ByteBuffer, channel: Channel) {
        val recoveryReporter = RecoveryReporter(msg)
        if (!electionMetaService.isLeader()) {
            logger.error("不是leader却收到了 RecoveryReport !??")
        } else {
            clusterRecoveryService.receive(fromServer, recoveryReporter.getCommited())
        }
    }

    // 不是 leader节点 需要在集群选举成功后，向leader发送 RecoveryReporter

    @NigateInject
    private lateinit var requestProcessCentreService: RequestProcessCentreService

    @NigateInject
    private lateinit var byteBufPreLogService: ByteBufPreLogService

    @NigateListener(onEvent = Event.CLUSTER_VALID)
    private fun whileClusterValid() {
        if (!electionMetaService.isLeader()) {
            requestProcessCentreService.send(
                electionMetaService.leader!!,
                RecoveryReporter(byteBufPreLogService.getCommitGAO())
            )
        }
    }
}