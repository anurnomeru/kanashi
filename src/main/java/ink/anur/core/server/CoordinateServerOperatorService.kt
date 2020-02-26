package ink.anur.core.server

import ink.anur.common.KanashiRunnable
import ink.anur.common.Shutdownable
import ink.anur.common.pool.DriverPool
import ink.anur.common.struct.common.AbstractStruct
import ink.anur.common.struct.enumerate.OperationTypeEnum
import ink.anur.core.coordinator.core.CoordinateMessageService
import ink.anur.core.struct.CoordinateRequest
import ink.anur.inject.Nigate
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.inject.NigatePostConstruct
import ink.anur.io.common.channel.ChannelService
import ink.anur.io.common.ShutDownHooker
import ink.anur.io.server.CoordinateServer
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelPipeline
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

/**
 * Created by Anur IjuoKaruKas on 2020/2/22
 *
 * 集群内通讯、协调服务器操作类服务端，负责协调相关的业务
 */
@NigateBean
class CoordinateServerOperatorService : KanashiRunnable(), Shutdownable {

    private val logger = LoggerFactory.getLogger(this::class.java)

    @NigateInject
    private lateinit var msgCenterService: CoordinateMessageService

    /**
     * 协调服务端
     */
    private var coordinateServer: CoordinateServer

    /**
     * CoordinateServerOperator 消费逻辑
     * 这里直接将解码后的 msg 丢入 HandlerPool
     */
    private val SERVER_MSG_CONSUMER: (ChannelHandlerContext, ByteBuffer) -> Unit = { ctx, msg ->
        var sign = 0
        try {
            sign = msg.getInt(AbstractStruct.TypeOffset)
        } catch (e: Exception) {
            e.printStackTrace()
        }

        val typeEnum = OperationTypeEnum.parseByByteSign(sign)
        DriverPool.offer(CoordinateRequest(msg, typeEnum, ctx.channel()))
    }

    private val SERVER_PIPELINE_CONSUME: (ChannelPipeline) -> Unit = { it.addFirst(UnRegister()) }

    /**
     * Coordinate 断开连接时，需要从 ChannelManager 移除管理
     */
    internal class UnRegister : ChannelInboundHandlerAdapter() {

        @Throws(Exception::class)
        override fun channelInactive(ctx: ChannelHandlerContext) {
            super.channelInactive(ctx)
            Nigate.getBeanByClass(ChannelService::class.java).getChannelHolder(ChannelService.ChannelType.COORDINATE).unRegister(ctx.channel())
        }
    }

    @NigatePostConstruct
    private fun init() = this.start()

    init {
        val sdh = ShutDownHooker("终止协调服务器的套接字接口 8080 的监听！")

        DriverPool.register(CoordinateRequest::class.java,
            8,
            300,
            TimeUnit.MILLISECONDS,
            {
                msgCenterService.receive(it.msg, it.typeEnum, it.channel)
            },
            null
        )

        this.coordinateServer = CoordinateServer(8080,
            sdh,
            SERVER_MSG_CONSUMER,
            SERVER_PIPELINE_CONSUME)
    }


    override fun shutDown() {
        coordinateServer.shutDown()
    }

    override fun run() {
        logger.info("协调服务器正在启动...")
        coordinateServer.start()
    }
}