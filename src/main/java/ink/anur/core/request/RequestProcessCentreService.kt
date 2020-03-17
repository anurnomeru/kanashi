package ink.anur.core.request

import ink.anur.common.Resetable
import ink.anur.common.pool.EventDriverPool
import ink.anur.common.struct.Request
import ink.anur.config.CoordinateConfig
import ink.anur.pojo.common.AbstractStruct
import ink.anur.pojo.common.AbstractTimedStruct
import ink.anur.pojo.enumerate.RequestTypeEnum
import ink.anur.core.common.RequestExtProcessor
import ink.anur.core.common.RequestMapping
import ink.anur.core.common.RequestProcessType
import ink.anur.core.response.ResponseProcessCentreService
import ink.anur.mutex.ReentrantReadWriteLocker
import ink.anur.exception.NetWorkException
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.inject.NigatePostConstruct
import ink.anur.io.common.channel.ChannelService
import ink.anur.service.RegisterHandleService
import ink.anur.timewheel.TimedTask
import ink.anur.timewheel.Timer
import io.netty.channel.Channel
import io.netty.util.internal.StringUtil
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

/**
 * Created by Anur IjuoKaruKas on 2020/2/24
 *
 * 消息控制中心，核心核心核心部件
 */
@NigateBean
class RequestProcessCentreService : ReentrantReadWriteLocker(), Resetable {

    @NigateInject
    private lateinit var channelService: ChannelService

    @NigateInject(useLocalFirst = true)
    private lateinit var coordinateConfig: CoordinateConfig

    @NigateInject
    private lateinit var msgSendService: ResponseProcessCentreService

    @NigateInject
    private lateinit var registerHandleService: RegisterHandleService

    private val logger = LoggerFactory.getLogger(this::class.java)

    /**
     * 此 map 用于保存接收到的信息的时间戳，如果收到旧的请求，则不作处理
     */
    private val receiveLog = mutableMapOf<String, MutableMap<RequestTypeEnum, Long?>>()

    /**
     * 注册所有的请求应该采用什么处理的映射
     */
    private val requestMappingRegister = mutableMapOf<RequestTypeEnum, RequestMapping>()

    /**
     * 注册所有请求的”回复“的映射
     */
    private val responseRequestRegister = mutableMapOf<RequestTypeEnum, RequestTypeEnum>()

    /**
     * 此 map 确保对一个服务发送某个消息，在收到回复之前，不可以再次对其发送消息。（有自动重发机制）
     */
    @Volatile
    private var inFlight = mutableMapOf<String, MutableMap<RequestTypeEnum, RequestExtProcessor?>>()

    /**
     * 此 map 用于重发请求
     */
    @Volatile
    private var reSendTask = mutableMapOf<String, MutableMap<RequestTypeEnum, TimedTask?>>()

    init {
        responseRequestRegister[RequestTypeEnum.REGISTER_RESPONSE] = RequestTypeEnum.REGISTER
        responseRequestRegister[RequestTypeEnum.RECOVERY_COMPLETE] = RequestTypeEnum.RECOVERY_REPORTER
        responseRequestRegister[RequestTypeEnum.FETCH_RESPONSE] = RequestTypeEnum.FETCH
    }

    @NigatePostConstruct
    private fun init() {
        EventDriverPool.register(Request::class.java,
            8,
            300,
            TimeUnit.MILLISECONDS
        ) {
            this.receive(it.msg, it.typeEnum, it.channel)
        }
    }

    override fun reset() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    /**
     * 注册 RequestMapping
     */
    fun registerRequestMapping(typeEnum: RequestTypeEnum, requestMapping: RequestMapping) {
        requestMappingRegister[typeEnum] = requestMapping
    }

    /**
     * 接收到消息如何处理
     */
    fun receive(msg: ByteBuffer, operationTypeEnum: RequestTypeEnum, channel: Channel?) {

        if (channel == null) {
            logger.error("????????????????????????????????????")
        } else {
            val requestTimestampCurrent = msg.getLong(AbstractTimedStruct.TimestampOffset)
            val serverName = channelService.getChannelName(channel)

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
                            removing(reSendTask, serverName, requestType)?.cancel()
                        }
                    }

                } catch (e: Exception) {
                    logger.error("在处理来自节点 $serverName 的 $operationTypeEnum 请求时出现异常", e)
                    writeLocker {
                        receiveLog.compute(serverName) { _, timestampMap ->
                            (timestampMap ?: mutableMapOf()).also { it.remove(operationTypeEnum) }
                        }
                    }
                }
            }
        }
    }

    /**
     * 此发送器保证【一个类型的消息】只能在收到回复前发送一次，类似于仅有 1 容量的Queue
     */
    fun send(serverName: String, msg: AbstractStruct, requestProcessor: RequestExtProcessor = RequestExtProcessor(), keepCurrentSendTask: Boolean = true, keepError: Boolean = false): Boolean {
        val typeEnum = msg.getRequestType()

        // 可以选择 keepCurrentSendTask = false 强制取消上次的任务
        return if (getting(inFlight, serverName, typeEnum)?.inFlight() == true && keepCurrentSendTask) {
            logger.debug("尝试创建发送到节点 $serverName 的 $typeEnum 任务失败，上次的指令还未收到 response")
            false
        } else {
            writeLocker {
                // 发送之前，移除之前可能遗留的任务
                removing(inFlight, serverName, typeEnum)?.complete()
                removing(reSendTask, serverName, typeEnum)?.cancel()
            }
            val error = sendImpl(serverName, msg, typeEnum, requestProcessor)
            if (keepError && error != null) {
                logger.error("尝试发送到节点 $serverName 的 $typeEnum 任务失败", error)
            }
            true
        }
    }

    /**
     * 真正发送消息的方法，内置了重发机制
     */
    private fun sendImpl(serverName: String, command: AbstractStruct, requestTypeEnum: RequestTypeEnum, requestProcessor: RequestExtProcessor): Throwable? {
        var throwable: Throwable? = null
        if (requestProcessor.inFlight()) {
            throwable = msgSendService.doSend(serverName, command)

            when (requestProcessor.requestProcessType) {
                // 只需要发送的类型直接移除重发任务
                RequestProcessType.SEND_ONCE -> {
                } // ignore

                // 需要发送并收到回复，但是不需要重发
                RequestProcessType.SEND_ONCE_THEN_NEED_RESPONSE -> {
                    writeLocker {
                        computing(inFlight, serverName, requestTypeEnum, requestProcessor)
                    }
                }

                // 需要发送并收到回复，也需要重发
                RequestProcessType.SEND_UNTIL_RESPONSE -> {
                    writeLocker {
                        val task = TimedTask(coordinateConfig.getReSendBackOfMs()) { sendImpl(serverName, command, requestTypeEnum, requestProcessor) }
                        computing(inFlight, serverName, requestTypeEnum, requestProcessor)
                        computing(reSendTask, serverName, requestTypeEnum, task)
                        Timer.getInstance()// 扔进时间轮不断重试，直到收到此消息的回复
                            .addTask(task)
                    }
                }
            }
        }
        return throwable
    }

    /**
     * 向 map -> 某 serverName 下的 typeEnum 写入 t
     */
    private fun <T> computing(map: MutableMap<String, MutableMap<RequestTypeEnum, T?>>, serverName: String, typeEnum: RequestTypeEnum, t: T?) {
        map.compute(serverName) { _, enums -> (enums ?: mutableMapOf()).also { it[typeEnum] = t } }
    }

    /**
     * 获取 map -> 某 serverName 下的 typeEnum 映射的 t
     */
    private fun <T> getting(map: MutableMap<String, MutableMap<RequestTypeEnum, T?>>, serverName: String, typeEnum: RequestTypeEnum): T? = map[serverName]?.let { it[typeEnum] }

    /**
     * 移除 map -> 某 serverName 下的 typeEnum 映射的 t
     */
    private fun <T> removing(map: MutableMap<String, MutableMap<RequestTypeEnum, T?>>, serverName: String, typeEnum: RequestTypeEnum): T? {
        val mutableMap = map[serverName]
        return mutableMap?.remove(typeEnum)
    }
}