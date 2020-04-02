package ink.anur.service.command

import ink.anur.common.Constant
import ink.anur.common.struct.KanashiNode
import ink.anur.config.InetConfig
import ink.anur.core.common.AbstractRequestMapping
import ink.anur.core.request.RequestProcessCentreService
import ink.anur.debug.Debugger
import ink.anur.exception.KanashiDatabaseException
import ink.anur.exception.MaxAttemptTimesException
import ink.anur.inject.NigateAfterBootStrap
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.pojo.command.KanashiCommandDto
import ink.anur.pojo.command.KanashiCommandResponse
import ink.anur.pojo.enumerate.RequestTypeEnum
import ink.anur.pojo.log.KanashiCommand
import ink.anur.pojo.log.common.CommandTypeEnum
import ink.anur.pojo.log.common.CommonApiTypeEnum
import ink.anur.pojo.log.common.StrApiTypeEnum
import ink.anur.pojo.log.common.TransactionTypeEnum
import io.netty.channel.Channel
import java.nio.ByteBuffer
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.random.Random

/**
 * Created by Anur IjuoKaruKas on 2020/3/29
 */
@NigateBean
class KanashiCommandResponseHandlerService : AbstractRequestMapping() {

    private val logger = Debugger(this.javaClass)

    @NigateInject
    private lateinit var requestProcessCentreService: RequestProcessCentreService

    @NigateInject(useLocalFirst = true)
    private lateinit var inetConfig: InetConfig

    @NigateAfterBootStrap
    private fun afterBootStrap() {
        nigateActivateLatch.countDown()
    }

    private val nigateActivateLatch = CountDownLatch(1)

    /**
     * 返回的结果会保存在这个 map 里面
     */
    private val responseMap = mutableMapOf<Long, KanashiCommandResponse>()

    /**
     * 通知调用线程唤醒的 map
     */
    private val notifyMap = mutableMapOf<Long, CountDownLatch>()

    @Volatile
    var gettingClusterLock = false

    /**
     * 缓存一份集群信息，因为集群信息是可能变化的，我们要保证在一次选举中，集群信息是不变的
     */
    @Volatile
    var clusters: List<KanashiNode>? = null

    var indexForGettingClusters = Random(100).nextInt()

    override fun typeSupport(): RequestTypeEnum {
        return RequestTypeEnum.COMMAND_RESPONSE
    }

    override fun handleRequest(fromServer: String, msg: ByteBuffer, channel: Channel) {
        val kanashiCommandResponse = KanashiCommandResponse(msg)

        synchronized(this) {
            responseMap[kanashiCommandResponse.msgTime] = kanashiCommandResponse
            notifyMap.remove(kanashiCommandResponse.msgTime)?.countDown()
        }
    }

    /**
     * 开始尝试去获取 cluster
     */
    fun startGetCluster() {
        if (gettingClusterLock) {
            return
        } else {
            getCluster()
        }
    }

    private fun getCluster() {
        val waitForResponse = CountDownLatch(1)
        val kanashiCommandDto = KanashiCommandDto("",
            KanashiCommand.generator(Long.MIN_VALUE, TransactionTypeEnum.SHORT, CommandTypeEnum.COMMON, CommonApiTypeEnum.GET_CLUSTER))

        var reCall = false

        val timeMillis = kanashiCommandDto.getTimeMillis()

        synchronized(this) {
            if (notifyMap[timeMillis] != null) {
                kanashiCommandDto.resetTimeMillis()
                reCall = true
            } else {
                notifyMap[timeMillis] = waitForResponse
            }
        }

        if (reCall) {
            getCluster()
            return
        }

        if (requestProcessCentreService.sendTo(Constant.SERVER, kanashiCommandDto)) {
            waitForResponse.await()
            setCluster(responseMap.remove(timeMillis)!!.kanashiEntry!!.getCluster())

        } else {
            logger.error("向配置节点请求集群信息失败，正在重试...")
            Thread.sleep(500)
            getCluster()
        }
    }

    private fun setCluster(nodes: List<KanashiNode>) {
        synchronized(this) {
            inetConfig.setClusters(nodes)
            clusters = nodes
            gettingClusterLock = false
        }
        logger.info("获取集群信息成功")
    }

    fun acquire(kanashiCommandDto: KanashiCommandDto, attemptTimes: Int): KanashiCommandResponse {

        println("开始 acquire")
        val sendTo: String = if (kanashiCommandDto.getKanashiCommand().isQueryCommand) {
            if (clusters != null) {
                clusters!![(clusters!!.size - 1) % indexForGettingClusters].serverName
            } else {
                Constant.SERVER
            }
        } else {
            if (clusters != null) {
                clusters!![0].serverName
            } else {
                Constant.SERVER
            }
        }


        if (attemptTimes == 0) {
            throw MaxAttemptTimesException("请求超过了最大尝试次数！")
        }

        nigateActivateLatch.await()
        val timeMillis = kanashiCommandDto.getTimeMillis()

        var reCall = false
        val waitForResponse = CountDownLatch(1)

        synchronized(this) {
            if (notifyMap[timeMillis] != null) {
                kanashiCommandDto.resetTimeMillis()
                reCall = true
            } else {
                notifyMap[timeMillis] = waitForResponse
            }
        }
        return if (reCall) {// 针对时间重复的要重新递归请求一次
            acquire(kanashiCommandDto, attemptTimes)
        } else {
            if (requestProcessCentreService.sendTo(sendTo, kanashiCommandDto)) {
                waitForResponse.await()
                val remove = responseMap.remove(timeMillis)
                if (remove == null) {
                    return acquire(kanashiCommandDto, attemptTimes - 1)
                } else {
                    if (remove.error) {
                        throw KanashiDatabaseException(remove.kanashiEntry?.getValueString() ?: "发送 command 请求后，服务端返回了未知错误")
                    }

                    if (remove.redirect) {
                        synchronized(this) {
                            clusters = remove.kanashiEntry!!.getCluster()
                        }
                        return acquire(kanashiCommandDto, attemptTimes - 1)
                    }
                }
                remove
            } else {
                // TODO 做有限次数的尝试
                Thread.sleep(200L)
                return acquire(kanashiCommandDto, attemptTimes - 1)
            }
        }
    }
}