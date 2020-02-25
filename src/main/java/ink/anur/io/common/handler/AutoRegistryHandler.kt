package ink.anur.io.common.handler

import ink.anur.common.struct.Register
import ink.anur.config.InetSocketAddressConfiguration
import ink.anur.core.msg.common.RequestExtProcessor
import ink.anur.core.msg.core.MsgCenterService
import ink.anur.core.msg.core.MsgSendService
import ink.anur.core.struct.KanashiNode
import ink.anur.inject.Nigate
import ink.anur.io.common.channel.ChannelService
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2020/2/25
 *
 * 自动注册的处理器
 */
class AutoRegistryHandler(val node: KanashiNode, private val doWhileRegistryComplete: List<() -> Unit>, private val doWhileUnRegistry: List<() -> Unit>) : ChannelInboundHandlerAdapter() {

    private val logger = LoggerFactory.getLogger(this::class.java)

    override fun channelActive(ctx: ChannelHandlerContext?) {
        super.channelActive(ctx)
        Nigate.getBeanByClass(ChannelService::class.java).getChannelHolder(ChannelService.ChannelType.COORDINATE).register(node.serverName, ctx!!.channel())
        val register = Register(Nigate.getBeanByClass(InetSocketAddressConfiguration::class.java).getLocalServerName())
        Nigate.getBeanByClass(MsgCenterService::class.java).send(node.serverName, register, AutoRegistryProcessor(doWhileRegistryComplete))
    }

    override fun channelInactive(ctx: ChannelHandlerContext?) {
        super.channelInactive(ctx)
        Nigate.getBeanByClass(ChannelService::class.java).getChannelHolder(ChannelService.ChannelType.COORDINATE).unRegister(node.serverName)

        logger.info("与协调器 节点 ${node.serverName} [${node.host}:${node.coordinatePort}] 的连接已断开")
        doWhileUnRegistry.forEach { it.invoke() }
    }

    inner class AutoRegistryProcessor(private val doWhileRegistryComplete: List<() -> Unit>) : RequestExtProcessor() {
        override fun howToConsumeResponse(response: ByteBuffer) {
            logger.info("成功与协调器 节点 ${node.serverName} [${node.host}:${node.coordinatePort}] 连接")
            doWhileRegistryComplete.forEach { it.invoke() }
        }

        override fun afterCompleteReceive() {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }
    }
}