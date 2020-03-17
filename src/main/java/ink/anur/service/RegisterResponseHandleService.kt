package ink.anur.service

import ink.anur.common.Constant
import ink.anur.core.common.AbstractRequestMapping
import ink.anur.debug.Debugger
import ink.anur.inject.Event
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.inject.NigateListenerService
import ink.anur.pojo.enumerate.RequestTypeEnum
import io.netty.channel.Channel
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2020/2/25
 *
 * 专门用于处理注册请求
 */
@NigateBean
class RegisterResponseHandleService : AbstractRequestMapping() {

    private val logger = Debugger(this.javaClass)

    @NigateInject
    private lateinit var nigateListenerService: NigateListenerService

    override fun typeSupport(): RequestTypeEnum = RequestTypeEnum.REGISTER_RESPONSE

    override fun handleRequest(fromServer: String, msg: ByteBuffer, channel: Channel) {
        if (fromServer == Constant.SERVER) {
            nigateListenerService.onEvent(Event.REGISTER_TO_SERVER)
        }
        logger.info("与节点 $fromServer 的连接已建立")
    }
}