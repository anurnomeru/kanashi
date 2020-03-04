package ink.anur.core.server

import ink.anur.common.KanashiRunnable
import ink.anur.common.Shutdownable
import ink.anur.common.pool.EventDriverPool
import ink.anur.common.struct.Request
import ink.anur.config.InetSocketAddressConfiguration
import ink.anur.core.request.RequestProcessCentreService
import ink.anur.inject.Nigate
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.inject.NigatePostConstruct
import ink.anur.io.common.ShutDownHooker
import ink.anur.io.common.channel.ChannelService
import ink.anur.io.server.CoordinateServer
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelPipeline
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

/**
 * Created by Anur IjuoKaruKas on 2020/2/22
 *
 * 集群内通讯、协调服务器操作类服务端，负责协调相关的业务
 */
@NigateBean
class CoordinateServerOperatorService : KanashiRunnable(), Shutdownable {

    @NigateInject
    private lateinit var msgCenterService: RequestProcessCentreService

    @NigateInject
    private lateinit var inetSocketAddressConfiguration: InetSocketAddressConfiguration

    private val logger = LoggerFactory.getLogger(this::class.java)

    /**
     * 协调服务端
     */
    private lateinit var coordinateServer: CoordinateServer

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
    private fun init() {
        val sdh = ShutDownHooker("终止协调服务器的套接字接口 ${inetSocketAddressConfiguration.getLocalServerCoordinatePort()} 的监听！")

        EventDriverPool.register(Request::class.java,
            8,
            300,
            TimeUnit.MILLISECONDS
        ) {
            msgCenterService.receive(it.msg, it.typeEnum, it.channel)
        }

        this.coordinateServer = CoordinateServer(inetSocketAddressConfiguration.getLocalServerCoordinatePort(),
            sdh,
            SERVER_PIPELINE_CONSUME)
        this.start()
    }


    override fun shutDown() {
        coordinateServer.shutDown()
    }

    override fun run() {
        logger.info("协调服务器正在启动...")
        coordinateServer.start()
    }
}