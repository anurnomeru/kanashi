package ink.anur.io.common.handler

import ink.anur.common.Constant
import ink.anur.common.struct.KanashiNode
import ink.anur.config.InetConfig
import ink.anur.core.common.RequestExtProcessor
import ink.anur.core.common.RequestProcessType
import ink.anur.core.request.RequestProcessCentreService
import ink.anur.exception.NetWorkException
import ink.anur.inject.Event
import ink.anur.inject.Nigate
import ink.anur.inject.NigateInject
import ink.anur.inject.NigateListener
import ink.anur.inject.NigateListenerService
import ink.anur.io.common.channel.ChannelService
import ink.anur.pojo.Register
import ink.anur.timewheel.TimedTask
import ink.anur.timewheel.Timer
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import org.slf4j.LoggerFactory
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Created by Anur IjuoKaruKas on 2020/2/25
 *
 * 自动注册的处理器
 */
class AutoRegistryHandler(private val node: KanashiNode) : ChannelInboundHandlerAdapter() {

    private val logger = LoggerFactory.getLogger(this::class.java)

    @NigateInject(useLocalFirst = true)
    private lateinit var inetConfig: InetConfig

    @NigateInject
    private lateinit var channelService: ChannelService

    @NigateInject
    private lateinit var msgCenterService: RequestProcessCentreService

    init {
        Nigate.injectOnly(this)
    }

    override fun channelActive(ctx: ChannelHandlerContext?) {
        super.channelActive(ctx)

        logger.info("正在向节点 $node 发送注册请求")
        channelService.register(node, ctx!!.channel())

        val register = Register(inetConfig.getLocalServerName())
        // TODO 这里可能有bug 如果server处理失败这里将无法连接
        msgCenterService.send(node.serverName, register, RequestExtProcessor(), false
    }

    override fun channelInactive(ctx: ChannelHandlerContext?) {
        super.channelInactive(ctx)
        channelService.unRegister(node.serverName)
        logger.info("与节点 $node 的连接已断开")
        ctx?.close()
    }
}