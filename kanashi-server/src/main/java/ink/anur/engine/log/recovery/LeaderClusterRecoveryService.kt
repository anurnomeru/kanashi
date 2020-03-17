package ink.anur.engine.log.recovery

import ink.anur.common.Resetable
import ink.anur.config.InetSocketAddressConfiguration
import ink.anur.core.common.RequestExtProcessor
import ink.anur.core.raft.ElectionMetaService
import ink.anur.core.request.RequestProcessCentreService
import ink.anur.debug.Debugger
import ink.anur.engine.log.common.LogService
import ink.anur.engine.log.fetch.AbstractFetcher
import ink.anur.engine.log.prelog.ByteBufPreLogService
import ink.anur.inject.Event
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.inject.NigateListener
import ink.anur.inject.NigateListenerService
import ink.anur.pojo.log.RecoveryComplete
import ink.anur.pojo.log.common.GenerationAndOffset
import ink.anur.pojo.server.FetchResponse
import ink.anur.util.TimeUtil
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Consumer

/**
 * Created by Anur IjuoKaruKas on 2020/3/12
 *
 * leader 负责集群日志恢复的控制器
 */
@NigateBean
class LeaderClusterRecoveryService : Resetable, AbstractFetcher() {

    private val logger = Debugger(this.javaClass)

    @NigateInject
    private lateinit var electMetaService: ElectionMetaService

    @NigateInject
    private lateinit var byteBufPreLogService: ByteBufPreLogService

    @NigateInject
    private lateinit var inetSocketAddressConfiguration: InetSocketAddressConfiguration

    @NigateInject
    private lateinit var nigateListenerService: NigateListenerService

    @NigateInject
    private lateinit var logService: LogService

    @NigateInject
    private lateinit var requestProcessCentreService: RequestProcessCentreService

    @NigateListener(onEvent = Event.CLUSTER_INVALID)
    override fun reset() {
        writeLocker {
            cancelFetchTask()
            logger.debug("LeaderClusterRecoveryManager RESET is triggered")
            RecoveryMap.clear()
            recoveryComplete = false
        }
    }

    /**
     * 当项目选主成功后，子节点需启动协调控制器去连接主节点
     * 将 recoveryComplete 设置为真，表示正在集群正在日志恢复
     */
    @NigateListener(onEvent = Event.CLUSTER_VALID)
    private fun whileClusterValid() {
        reset()
        RecoveryTimer = TimeUtil.getTime()
        if (electMetaService.isLeader()) {
            receive(electMetaService.leader!!, byteBufPreLogService.getCommitGAO())
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
        } else {
            sendRecoveryComplete(serverName, latestGao)
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

    override fun howToConsumeFetchResponse(fetchFrom: String, fetchResponse: FetchResponse) {
        logger.trace("收到节点 {} 返回的 FETCH_RESPONSE", fetchFrom)

        electMetaService.takeIf { !it.isLeader() }?.run { logger.error("出现了不应该出现的情况！喵喵锤！") }
        val read = fetchResponse.read()
        val iterator = read.iterator()

        val gen = fetchResponse.generation
        val fetchToGen = fetchTo!!.generation

        var start: Long? = null
        var end: Long? = null

        iterator.forEach {

            if (start == null) start = it.offset
            end = it.offset

            logService
                .append(gen, it.offset, it.operation)

            if (gen == fetchToGen) {
                val offset = it.offset
                val fetchToOffset = fetchTo!!.offset
                if (offset == fetchToOffset) {// 如果已经同步完毕，则通知集群同步完成
                    cancelFetchTask()
                    shuttingWhileRecoveryComplete()
                }
            }
        }

        byteBufPreLogService.cover(GenerationAndOffset(gen, end!!))
        logger.debug("集群日志恢复：追加 gen = {$gen} offset-start {$start} end {$end} 的日志段完毕")
    }
}