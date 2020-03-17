package ink.anur.service.log

import ink.anur.common.Resetable
import ink.anur.config.CoordinateConfiguration
import ink.anur.config.InetSocketAddressConfiguration
import ink.anur.core.common.AbstractTimedRequestMapping
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
import ink.anur.mutex.ReentrantReadWriteLocker
import ink.anur.pojo.enumerate.RequestTypeEnum
import ink.anur.pojo.log.RecoveryComplete
import ink.anur.pojo.log.RecoveryReporter
import ink.anur.pojo.log.common.GenerationAndOffset
import ink.anur.pojo.server.Fetch
import ink.anur.pojo.server.FetchResponse
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

    // 不是 leader节点 需要在集群选举成功后，向leader发送 RecoveryReporter

    @NigateInject
    private lateinit var requestProcessCentreService: RequestProcessCentreService

    @NigateInject
    private lateinit var byteBufPreLogService: ByteBufPreLogService

    @NigateListener(onEvent = Event.CLUSTER_VALID)
    private fun whileClusterValid() {
        reset()
        RecoveryTimer = TimeUtil.getTime()

        if (!electionMetaService.isLeader()) {
            requestProcessCentreService.send(electionMetaService.leader!!, RecoveryReporter(byteBufPreLogService.getCommitGAO()))
        } else {
            // 当项目选主成功后，子节点需启动协调控制器去连接主节点
            // 将 recoveryComplete 设置为真，表示正在集群正在日志恢复
            receive(electMetaService.leader!!, byteBufPreLogService.getCommitGAO())
        }
    }


    @NigateInject
    private lateinit var electMetaService: ElectionMetaService

    @NigateInject
    private lateinit var inetSocketAddressConfiguration: InetSocketAddressConfiguration

    @NigateInject
    private lateinit var nigateListenerService: NigateListenerService

    @NigateInject
    private lateinit var logService: LogService


    @NigateListener(onEvent = Event.CLUSTER_INVALID)
    override fun reset() {
        logger.debug("LeaderClusterRecoveryManager RESET is triggered")
        locker.writeLocker {
            cancelTask()
            RecoveryMap.clear()
            recoveryComplete = false
        }
    }


    private var RecoveryTimer: Long = 0

    @Volatile
    private var waitShutting = ConcurrentHashMap<String, GenerationAndOffset>()

    @Volatile
    private var RecoveryMap = ConcurrentHashMap<String, GenerationAndOffset>()

    @Volatile
    private var recoveryComplete = false

    @Volatile
    private var fetchTo: GenerationAndOffset? = null

    fun receive(serverName: String, latestGao: GenerationAndOffset) {
        logger.info("节点 $serverName 提交了其最大进度 $latestGao ")
        RecoveryMap[serverName] = latestGao

        if (!recoveryComplete) {
            if (RecoveryMap.size >= electMetaService.quorum) {
                var latest: MutableMap.MutableEntry<String, GenerationAndOffset>? = null
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
                    val serverName = latest!!.key
                    val GAO = latest!!.value
                    val node = inetSocketAddressConfiguration.getNode(serverName)
                    fetchTo = GAO

                    logger.info("已有过半节点提交了最大进度，集群最大进度于节点 $serverName ，进度为 $GAO ，Leader 将从其同步最新数据")

                    /*
                     * 当连接上子节点，开始日志同步，同步具体逻辑在 {@link #howToConsumeFetchResponse}
                     */
                    super.startToFetchFrom(serverName)
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
    private fun sendFetchMessage(myVersion: Long, fetchFrom: String) {
        locker.writeLockSupplier {
            if (!super.isCancel()) {
                requestProcessCentreService.send(fetchFrom, Fetch(byteBufPreLogService.getPreLogGAO()))
            }
        }
        try {
            fetchPreLogTask?.takeIf { !it.isCancel }?.run {
                ApisManager.send(fetchFrom,
                    Fetcher(ByteBufPreLogManager.getPreLogGAO()),
                    RequestProcessor(Consumer {
                        readLocker {
                            val fetchResponse = FetchResponse(it)
                            if (fetchResponse.generation != FetchResponse.Invalid) {
                                howToConsumeFetchResponse(fetchFrom, fetchResponse)
                            }
                            rebuildFetchTask(myVersion, fetchFrom)
                        }
                    },
                        null
                    ))
            }
        } finally {
            fetchLock.unlock()
        }
    }


    private fun shuttingWhileRecoveryComplete() {
        recoveryComplete = true
        waitShutting.entries.forEach(Consumer { sendRecoveryComplete(it.key, it.value) })
        nigateListenerService.onEvent(Event.RECOVERY_COMPLETE)
    }

    private fun sendRecoveryComplete(serverName: String, latestGao: GenerationAndOffset) {
        if (recoveryComplete) {
            doSendRecoveryComplete(serverName, latestGao)
        } else {
            waitShutting[serverName] = latestGao
        }
    }

    private fun doSendRecoveryComplete(serverName: String, latestGao: GenerationAndOffset) {
        val GAO = GenerationAndOffset(latestGao.generation, logService.loadGenLog(latestGao.generation)!!.currentOffset)
        requestProcessCentreService.send(serverName, RecoveryComplete(GAO))
    }

    fun howToConsumeFetchResponse(fetchFrom: String, fetchResponse: FetchResponse) {

    }

    override fun internal(): Long {
        return coordinateConfiguration.getReSendBackOfMs()
    }
}