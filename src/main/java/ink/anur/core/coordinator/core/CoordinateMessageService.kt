package ink.anur.core.coordinator.core

import ink.anur.common.Resetable
import ink.anur.struct.common.AbstractStruct
import ink.anur.struct.common.AbstractTimedStruct
import ink.anur.struct.enumerate.OperationTypeEnum
import ink.anur.config.CoordinateConfiguration
import ink.anur.core.coordinator.common.RequestExtProcessor
import ink.anur.core.coordinator.common.RequestMapping
import ink.anur.core.rentrant.ReentrantReadWriteLocker
import ink.anur.exception.NetWorkException
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.io.common.channel.ChannelService
import ink.anur.service.RegisterHandleService
import ink.anur.timewheel.TimedTask
import ink.anur.timewheel.Timer
import io.netty.channel.Channel
import io.netty.util.internal.StringUtil
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2020/2/24
 *
 * 消息控制中心，核心核心核心部件
 */
@NigateBean
class CoordinateMessageService : ReentrantReadWriteLocker(), Resetable {

    @NigateInject
    private lateinit var channelService: ChannelService

    @NigateInject
    private lateinit var coordinateConfiguration: CoordinateConfiguration

    @NigateInject
    private lateinit var msgSendService: CoordinateSenderService

    @NigateInject
    private lateinit var registerHandleService: RegisterHandleService

    private val logger = LoggerFactory.getLogger(this::class.java)

    /**
     * 此 map 用于保存接收到的信息的时间戳，如果收到旧的请求，则不作处理
     */
    private val receiveLog = mutableMapOf<String, MutableMap<OperationTypeEnum, Long?>>()

    /**
     * 注册所有的请求应该采用什么处理的映射
     */
    private val requestMappingRegister = mutableMapOf<OperationTypeEnum, RequestMapping>()

    /**
     * 注册所有请求的”回复“的映射
     */
    private val responseRequestRegister = mutableMapOf<OperationTypeEnum, OperationTypeEnum>()

    /**
     * 此 map 确保对一个服务发送某个消息，在收到回复之前，不可以再次对其发送消息。（有自动重发机制）
     */
    @Volatile
    private var inFlight = mutableMapOf<String, MutableMap<OperationTypeEnum, RequestExtProcessor?>>()

    /**
     * 此 map 用于重发请求
     */
    @Volatile
    private var reSendTask = mutableMapOf<String, MutableMap<OperationTypeEnum, TimedTask?>>()

    init {
        responseRequestRegister[OperationTypeEnum.REGISTER_RESPONSE] = OperationTypeEnum.REGISTER
    }

    override fun reset() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    /**
     * 注册 RequestMapping
     */
    fun registerRequestMapping(typeEnum: OperationTypeEnum, requestMapping: RequestMapping) {
        requestMappingRegister[typeEnum] = requestMapping
    }

    /**
     * 接收到消息如何处理
     */
    fun receive(msg: ByteBuffer, operationTypeEnum: OperationTypeEnum, channel: Channel?) {

        if (channel == null) {
            logger.error("????????????????????????????????????")
        } else {
            val requestTimestampCurrent = msg.getLong(AbstractTimedStruct.TimestampOffset)
            val serverName = channelService.getChannelHolder(ChannelService.ChannelType.COORDINATE).getChannelName(channel)

            // serverName 是不会为空的，但是有一种情况例外，便是服务还未注册时 这里做特殊处理
            when {
                serverName == null -> registerHandleService.handleRequest("", msg, channel)
                writeLockSupplierCompel {
                    var changed = false
                    receiveLog.compute(serverName) { _, timestampMap ->
                        (timestampMap ?: mutableMapOf()).also {
                            it.compute(operationTypeEnum) { _, timestampBefore ->
                                changed = (timestampBefore == null || requestTimestampCurrent > timestampBefore)
                                if (changed) requestTimestampCurrent else timestampBefore
                            }
                        }
                    }
                    changed
                } -> try {
                    val requestMapping = requestMappingRegister[operationTypeEnum]
                    if (requestMapping != null) {
                        requestMapping.handleRequest(serverName, msg, channel)// 收到正常的请求
                    } else {

                        /*
                         *  默认请求处理，也就是 response 处理
                         * response 的处理非常简单：
                         *
                         * 1、触发 complete
                         * 2、移除 MAPPING
                         */
                        val requestType = responseRequestRegister[operationTypeEnum]!!
                        logger.trace("收到来自节点 {} 关于 {} 的 response", serverName, requestType.name)

                        if (StringUtil.isNullOrEmpty(serverName)) {
                            throw NetWorkException("收到了来自已断开连接节点 $serverName 关于 ${requestType.name} 的无效 response")
                        }

                        readLockSupplier { getting(inFlight, serverName, requestType) }?.complete(msg)
                        writeLocker {
                            removing(inFlight, serverName, requestType)?.complete()
                            removing(reSendTask, serverName, requestType)
                        }
                    }

                } catch (e: Exception) {
                    logger.warn("在处理来自节点 {} 的 {} 请求时出现异常，可能原因 {}", serverName, operationTypeEnum, e.message)
                    writeLocker {
                        receiveLog.compute(serverName) { _, timestampMap ->
                            (timestampMap ?: mutableMapOf()).also { it.remove(operationTypeEnum) }
                        }
                    }
                    e.printStackTrace()
                }
            }
        }
    }

    /**
     * 此发送器保证【一个类型的消息】只能在收到回复前发送一次，类似于仅有 1 容量的Queue
     */
    fun send(serverName: String, msg: AbstractStruct, requestProcessor: RequestExtProcessor = RequestExtProcessor()): Boolean {
        val typeEnum = msg.getOperationTypeEnum()
        return if (getting(inFlight, serverName, typeEnum) != null) {
            logger.debug("尝试创建发送到节点 {} 的 {} 任务失败，上次的指令还未收到 response", serverName, typeEnum.name)
            false
        } else {
            writeLocker {
                // 发送之前，首先将任务加入 inflight，移除之前可能留下的发送任务
                computing(inFlight, serverName, typeEnum, requestProcessor)
                removing(reSendTask, serverName, typeEnum)?.cancel()
            }
            sendImpl(serverName, msg, typeEnum, requestProcessor)
                ?.let { logger.error("向节点 $serverName 发送 $typeEnum 请求失败! ${it.cause?.let { c -> "[原因：$c]" } ?: ""} ${it.message?.let { c -> "[msg：$c]" } ?: ""}") }
            true
        }
    }

    /**
     * 真正发送消息的方法，内置了重发机制
     */
    private fun sendImpl(serverName: String, command: AbstractStruct, operationTypeEnum: OperationTypeEnum, requestProcessor: RequestExtProcessor): Throwable? {
        var throwable: Throwable? = null
        if (!requestProcessor.isComplete()) {
            throwable = msgSendService.doSend(serverName, command)
        }
        if (!requestProcessor.sendUntilReceiveResponse) { // 是不需要回复的类型，直接移除所有任务，发出去了就完事了
            writeLocker {
                removing(inFlight, serverName, operationTypeEnum)?.complete()
                removing(reSendTask, serverName, operationTypeEnum)?.cancel()
            }
        } else {
            if (getting(inFlight, serverName, operationTypeEnum)?.isComplete() == false) {// 如果还没收到回复，则重新拟定重发定时任务
                val task = TimedTask(coordinateConfiguration.getFetchBackOfMs().toLong()) { sendImpl(serverName, command, operationTypeEnum, requestProcessor) }
                writeLocker {
                    computing(reSendTask, serverName, operationTypeEnum, task)
                }
                Timer.getInstance()// 扔进时间轮不断重试，直到收到此消息的回复
                    .addTask(task)
            }
        }

        return throwable
    }

    /**
     * 向 map -> 某 serverName 下的 typeEnum 写入 t
     */
    private fun <T> computing(map: MutableMap<String, MutableMap<OperationTypeEnum, T?>>, serverName: String, typeEnum: OperationTypeEnum, t: T?) {
        map.compute(serverName) { _, enums -> (enums ?: mutableMapOf()).also { it[typeEnum] = t } }
    }

    /**
     * 获取 map -> 某 serverName 下的 typeEnum 映射的 t
     */
    private fun <T> getting(map: MutableMap<String, MutableMap<OperationTypeEnum, T?>>, serverName: String, typeEnum: OperationTypeEnum): T? = map[serverName]?.let { it[typeEnum] }

    /**
     * 移除 map -> 某 serverName 下的 typeEnum 映射的 t
     */
    private fun <T> removing(map: MutableMap<String, MutableMap<OperationTypeEnum, T?>>, serverName: String, typeEnum: OperationTypeEnum): T? {
        val mutableMap = map[serverName]
        return mutableMap?.remove(typeEnum)
    }
}