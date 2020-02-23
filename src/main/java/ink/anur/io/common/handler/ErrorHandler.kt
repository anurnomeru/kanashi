package ink.anur.io.common.handler

import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter

/**
 * Created by Anur IjuoKaruKas on 2020/2/22
 *
 * 将 channel 中发生的错误暴露出来
 */
class ErrorHandler : ChannelInboundHandlerAdapter() {

    override fun exceptionCaught(ctx: ChannelHandlerContext?, cause: Throwable?) {
        cause?.printStackTrace()
    }
}