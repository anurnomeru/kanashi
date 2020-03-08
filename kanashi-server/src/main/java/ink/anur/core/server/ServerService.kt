package ink.anur.core.server

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

/**
 * Created by Anur IjuoKaruKas on 2020/2/22
 *
 * 集群内通讯、协调服务器操作类服务端，负责协调相关的业务
 */
@NigateBean
class ServerService : Shutdownable {

    @NigateInject
    private lateinit var inetSocketAddressConfiguration: InetSocketAddressConfiguration

    /**
     * 协调服务端
     */
    private lateinit var coordinateServer: CoordinateServer

    @NigatePostConstruct
    private fun init() {
        val sdh = ShutDownHooker("终止协调服务器的套接字接口 ${inetSocketAddressConfiguration.getLocalCoordinatePort()} 的监听！")
        this.coordinateServer = CoordinateServer(inetSocketAddressConfiguration.getLocalCoordinatePort(),
            sdh)
        coordinateServer.start()
    }


    override fun shutDown() {
        coordinateServer.shutDown()
    }
}