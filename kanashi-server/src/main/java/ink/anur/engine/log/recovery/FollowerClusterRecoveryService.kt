package ink.anur.engine.log.recovery

import ink.anur.core.common.RequestExtProcessor
import ink.anur.core.raft.ElectionMetaService
import ink.anur.core.request.RequestProcessCentreService
import ink.anur.debug.Debugger
import ink.anur.engine.log.common.LogService
import ink.anur.engine.log.prelog.ByteBufPreLogService
import ink.anur.inject.Event
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.inject.NigateListener
import ink.anur.inject.NigateListenerService
import ink.anur.pojo.log.RecoveryComplete
import ink.anur.pojo.log.RecoveryReporter

/**
 * Created by Anur IjuoKaruKas on 4/9/2019
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
class FollowerClusterRecoveryService {

    private val logger = Debugger(this.javaClass)

    @NigateInject
    private lateinit var electionMetaService: ElectionMetaService

    @NigateInject
    private lateinit var requestProcessCentreService: RequestProcessCentreService

    @NigateInject
    private lateinit var byteBufPreLogService: ByteBufPreLogService

    @NigateInject
    private lateinit var logService: LogService

    @NigateInject
    private lateinit var nigateListenerService: NigateListenerService

    @NigateListener(onEvent = Event.CLUSTER_VALID)
    private fun whileClusterValid() {
        if (!electionMetaService.isLeader()) {
            requestProcessCentreService.send(
                electionMetaService.leader!!,
                RecoveryReporter(byteBufPreLogService.getCommitGAO()),
                RequestExtProcessor(
                    {
                        val recoveryComplete = RecoveryComplete(it)
                        val clusterGAO = recoveryComplete.getCommited()
                        val localGAO = byteBufPreLogService.getCommitGAO()
                        if (localGAO > clusterGAO) {
                            logger.debug("当前世代集群日志最高为 $clusterGAO ，比本地 $localGAO 小，故需删除大于集群日志的所有日志")
                            logService.discardAfter(clusterGAO)
                        }

                        logger.info("集群已经恢复正常，开始通知 Fetcher 进行日志同步")

                        /*
                         * 当集群同步完毕，通知 RECOVERY_COMPLETE
                         */
                        nigateListenerService.onEvent(Event.RECOVERY_COMPLETE)
                    }
                )
            )
        }
    }
}