package ink.anur.io.common

import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2020/2/22
 *
 * 定义了如何去消费一个 byteBuffer ，此 Handler 用于注册到 pipeLine，我们无需再额外创建 pipeLine，只需定义消费即可
 */
class ByteBufferMsgConsumerHandler(private val howToConsumeByteBuffer: (ChannelHandlerContext, ByteBuffer) -> Unit) : SimpleChannelInboundHandler<ByteBuffer>() {

    private val logger = LoggerFactory.getLogger(ByteBufferMsgConsumerHandler::class.java)

    override fun channelRead0(p0: ChannelHandlerContext?, p1: ByteBuffer?) {
        if (p0 == null || p1 == null) {
            logger.error("Channel HandlerContext or its byte buffer in pipeline is null")
        } else {
            howToConsumeByteBuffer.invoke(p0, p1)
        }
    }
}




