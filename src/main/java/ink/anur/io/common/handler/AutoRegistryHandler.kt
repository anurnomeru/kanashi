package ink.anur.io.common.handler

import ink.anur.common.struct.KanashiNode
import ink.anur.config.InetConfig
import ink.anur.core.common.RequestExtProcessor
import ink.anur.core.common.RequestProcessType
import ink.anur.core.request.RequestProcessCentreService
import ink.anur.exception.NetWorkException
import ink.anur.inject.Nigate
import ink.anur.inject.NigateInject
import ink.anur.inject.NigateListener
import ink.anur.io.common.channel.ChannelService
import ink.anur.pojo.Register
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
        val connectCDL = CountDownLatch(1)
        channelService.register(node, ctx!!.channel())

        val register = Register(inetConfig.getLocalServerName())
        msgCenterService.send(node.serverName, register, RequestExtProcessor({
            connectCDL.countDown()
            logger.info("与节点 $node 的连接已建立")
        }, RequestProcessType.SEND_ONCE_THEN_NEED_RESPONSE), false)

        if (!connectCDL.await(2, TimeUnit.SECONDS)) {
            throw NetWorkException("尝试连接服务 $node 失败，两秒内没有收到注册回复！")
        }
    }

    override fun channelInactive(ctx: ChannelHandlerContext?) {
        super.channelInactive(ctx)
        channelService.unRegister(node.serverName)
        logger.info("与节点 $node 的连接已断开")
        ctx?.close()
    }
}