package ink.anur.io.common.handler

import ink.anur.config.InetSocketAddressConfiguration
import ink.anur.core.coordinator.common.RequestExtProcessor
import ink.anur.core.coordinator.core.CoordinateCentreService
import ink.anur.inject.Nigate
import ink.anur.inject.NigateInject
import ink.anur.io.common.channel.ChannelService
import ink.anur.struct.Register
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import org.slf4j.LoggerFactory

/**
 * Created by Anur IjuoKaruKas on 2020/2/25
 *
 * 自动注册的处理器
 */
class AutoRegistryHandler(private val serverName: String, private val host: String, private val port: Int) : ChannelInboundHandlerAdapter() {

    private val logger = LoggerFactory.getLogger(this::class.java)

    @NigateInject
    private lateinit var channelService: ChannelService

    @NigateInject
    private lateinit var inetSocketAddressConfiguration: InetSocketAddressConfiguration

    @NigateInject
    private lateinit var msgCenterService: CoordinateCentreService

    init {
        Nigate.initInject(this)
    }

    override fun channelActive(ctx: ChannelHandlerContext?) {
        super.channelActive(ctx)
        val register = Register(inetSocketAddressConfiguration.getLocalServerName())
        logger.info("正在向节点 $serverName [$host:$port] 发送注册请求")
        channelService.getChannelHolder(ChannelService.ChannelType.COORDINATE).register(serverName, ctx!!.channel())
        msgCenterService.send(serverName, register, RequestExtProcessor(true, {
            logger.info("与节点 $serverName [$host:$port] 的连接已建立")
        }))
    }

    override fun channelInactive(ctx: ChannelHandlerContext?) {
        super.channelInactive(ctx)
        channelService.getChannelHolder(ChannelService.ChannelType.COORDINATE).unRegister(serverName)
        logger.info("与节点 $serverName [$host:$port] 的连接已断开")
    }
}