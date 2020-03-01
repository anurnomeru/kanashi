package ink.anur.io.common.handler

import ink.anur.common.pool.DriverPool
import ink.anur.core.struct.CoordinateRequest
import ink.anur.struct.common.AbstractStruct
import ink.anur.struct.enumerate.OperationTypeEnum
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2020/3/1
 *
 * 将收到的消息扔进 DriverPool，再给到 CoordinateMessageService 消费
 */
class CoordinateDriverPoolHandler : SimpleChannelInboundHandler<ByteBuffer>() {

    private val logger = LoggerFactory.getLogger(this::class.java)

    override fun channelRead0(ctx: ChannelHandlerContext?, msg: ByteBuffer?) {
        if (ctx != null && msg != null) {
            var sign = 0
            try {
                sign = msg.getInt(AbstractStruct.TypeOffset)
            } catch (e: Exception) {
                e.printStackTrace()
            }

            val typeEnum = OperationTypeEnum.parseByByteSign(sign)
            DriverPool.offer(CoordinateRequest(msg, typeEnum, ctx.channel()))
        } else {
            logger.error("Channel HandlerContext or its byte buffer in pipeline is null")
        }
    }
}