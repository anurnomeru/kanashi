package ink.anur.service.log

import ink.anur.common.Resetable
import ink.anur.config.CoordinateConfiguration
import ink.anur.core.common.AbstractTimedRequestMapping
import ink.anur.core.raft.ElectionMetaService
import ink.anur.core.request.RequestProcessCentreService
import ink.anur.debug.Debugger
import ink.anur.engine.log.LogService
import ink.anur.engine.prelog.ByteBufPreLogService
import ink.anur.inject.Event
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.inject.NigateListener
import ink.anur.inject.NigateListenerService
import ink.anur.mutex.ReentrantReadWriteLocker
import ink.anur.pojo.enumerate.RequestTypeEnum
import ink.anur.pojo.log.RecoveryComplete
import ink.anur.pojo.log.RecoveryReporter
import ink.anur.pojo.log.common.GenerationAndOffset
import ink.anur.pojo.command.Fetch
import ink.anur.util.TimeUtil
import io.netty.channel.Channel
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Consumer

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
class RecoveryReportHandleService : AbstractTimedRequestMapping(), Resetable {

    private val logger = Debugger(this::class.java)

    @NigateInject
    private lateinit var coordinateConfiguration: CoordinateConfiguration

    @NigateInject
    private lateinit var electionMetaService: ElectionMetaService

    @NigateInject
    private lateinit var requestProcessCentreService: RequestProcessCentreService

    @NigateInject
    private lateinit var byteBufPreLogService: ByteBufPreLogService

    @NigateInject
    private lateinit var nigateListenerService: NigateListenerService

    @NigateInject
    private lateinit var logService: LogService

    private val locker = ReentrantReadWriteLocker()

    override fun typeSupport(): RequestTypeEnum {
        return RequestTypeEnum.RECOVERY_REPORTER
    }

    override fun handleRequest(fromServer: String, msg: ByteBuffer, channel: Channel) {
        val recoveryReporter = RecoveryReporter(msg)
        if (!electionMetaService.isLeader()) {
            logger.error("不是leader却收到了 RecoveryReport !??")
        } else {
            this.receive(fromServer, recoveryReporter.getCommited())
        }
    }

    @NigateListener(onEvent = Event.CLUSTER_VALID)
    private fun whileClusterValid() {
        // 集群可用后选重置所有状态
        reset()

        // 当集群可用时， leader节点会受到一个来自自己的 recovery Report
        if (electionMetaService.isLeader()) {
            this.receive(electionMetaService.leader!!, byteBufPreLogService.getCommitGAO())
        } else {
            // 如果不是 leader，则需要各个节点汇报自己的 log 进度，给 leader 发送  recovery Report
            requestProcessCentreService.send(electionMetaService.leader!!, RecoveryReporter(byteBufPreLogService.getCommitGAO()))
        }
    }

    @NigateListener(onEvent = Event.RECOVERY_COMPLETE)
    private fun whileRecoveryComplete() {
        if (!electionMetaService.isLeader()) {
            startToFetchFrom(electionMetaService.leader!!)
        }
    }

    @NigateListener(onEvent = Event.CLUSTER_INVALID)
    override fun reset() {
        logger.debug("RecoveryReportHandlerService RESET is triggered")
        locker.writeLocker {
            super.cancelTask()
            RecoveryMap.clear()
            recoveryComplete = false
            RecoveryTimer = TimeUtil.getTime()
        }
    }

    /**
     * 单纯记录 Recovery 需要花费多少时间
     */
    private var RecoveryTimer: Long = 0

    /**
     * 这是个通知集合，表示当受到节点的 RecoveryReport 后，会将最新的 GAO 通知给对方
     */
    @Volatile
    private var waitShutting = ConcurrentHashMap<String, GenerationAndOffset>()

    /**
     * 记录各个节点在 Recovery 时上报的最新 GAO 是多少
     */
    @Volatile
    private var RecoveryMap = ConcurrentHashMap<String, GenerationAndOffset>()

    /**
     * 是否已经完成了 Recovery
     */
    @Volatile
    private var recoveryComplete = false

    /**
     * 在半数节点上报了 GAO 后，leader 决定去同步消息到这个进度
     */
    @Volatile
    private var fetchTo: GenerationAndOffset? = null

    fun receive(serverName: String, latestGao: GenerationAndOffset) {
        logger.info("节点 $serverName 提交了其最大进度 $latestGao ")
        RecoveryMap[serverName] = latestGao

        if (!recoveryComplete) {
            if (RecoveryMap.size >= electionMetaService.quorum) {
                var latest: MutableMap.MutableEntry<String, GenerationAndOffset>? = null

                // 找寻提交的所有的提交的 GAO 里面最大的
                RecoveryMap.entries.forEach(Consumer {
                    if (latest == null || it.value > latest!!.value) {
                        latest = it
                    }
                })

                if (latest!!.value == byteBufPreLogService.getCommitGAO()) {
                    val cost = TimeUtil.getTime() - RecoveryTimer
                    logger.info("已有过半节点提交了最大进度，且集群最大进度 ${latest!!.value} 与 Leader 节点相同，集群已恢复，耗时 $cost ms ")
                    shuttingWhileRecoveryComplete()
                } else {
                    val latestNode = latest!!.key
                    val latestGAO = latest!!.value
                    fetchTo = latestGAO

                    logger.info("已有过半节点提交了最大进度，集群最大进度于节点 $serverName ，进度为 $latestGAO ，Leader 将从其同步最新数据")

                    // 开始进行 fetch
                    startToFetchFrom(latestNode)
                }
            }
        }

        sendRecoveryComplete(serverName, latestGao)
    }

    /**
     * 主要负责定时 Fetch 消息
     *
     * 新建一个 Fetcher 用于拉取消息，将其发送给 Leader，并在收到回调后，调用 CONSUME_FETCH_RESPONSE 消费回调，且重启拉取定时任务
     */
    private fun startToFetchFrom(fetchFrom: String) {
        logger.info("开始向 $fetchFrom fetch 消息 -->")
        rebuildTask { requestProcessCentreService.send(fetchFrom, Fetch(byteBufPreLogService.getPreLogGAO())) }
    }

    /**
     * 触发向各个节点发送 RecoveryComplete，发送完毕后 触发 RECOVERY_COMPLETE
     * // TODO 避免 client 重复触发！！
     */
    private fun shuttingWhileRecoveryComplete() {
        recoveryComplete = true
        waitShutting.entries.forEach(Consumer { sendRecoveryComplete(it.key, it.value) })
        nigateListenerService.onEvent(Event.RECOVERY_COMPLETE)
    }

    /**
     * 向节点发送 RecoveryComplete 告知已经 Recovery 完毕
     *
     * 如果还没同步完成，则将其暂存到 waitShutting 中
     */
    private fun sendRecoveryComplete(serverName: String, latestGao: GenerationAndOffset) {
        if (recoveryComplete) {
            requestProcessCentreService.send(serverName, RecoveryComplete(
                GenerationAndOffset(latestGao.generation, logService.loadGenLog(latestGao.generation)!!.currentOffset)
            ))
        } else {
            waitShutting[serverName] = latestGao
        }
    }

    override fun internal(): Long {
        return coordinateConfiguration.getReSendBackOfMs()
    }
}