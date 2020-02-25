package ink.anur.core.msg.service

import ink.anur.common.struct.Register
import ink.anur.common.struct.RegisterResponse
import ink.anur.common.struct.enumerate.OperationTypeEnum
import ink.anur.config.InetSocketAddressConfiguration
import ink.anur.core.msg.common.AbstractRequestMapping
import ink.anur.core.msg.core.MsgCenterService
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.io.common.channel.ChannelService
import io.netty.channel.Channel
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2020/2/25
 *
 * 专门用于处理注册请求
 */
@NigateBean
class RegisterHandlerService : AbstractRequestMapping() {

    private val logger = LoggerFactory.getLogger(this::class.java)

    @NigateInject
    private lateinit var channelService: ChannelService

    @NigateInject
    private lateinit var msgCenterService: MsgCenterService

    @NigateInject
    private lateinit var inetSocketAddressConfiguration: InetSocketAddressConfiguration

    override fun typeSupport(): OperationTypeEnum = OperationTypeEnum.REGISTER

    override fun handleRequest(fromServer: String, msg: ByteBuffer, channel: Channel) {
        val register = Register(msg)

        channelService
            .getChannelHolder(ChannelService.ChannelType.COORDINATE)
            .register(register.getServerName(), channel)
        logger.info("协调节点 {} 已注册到本节点", register.getServerName())

        msgCenterService.send(fromServer, RegisterResponse(inetSocketAddressConfiguration.getLocalServerName()))
    }
}