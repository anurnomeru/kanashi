package ink.anur.io.server

import ink.anur.io.common.handler.ByteBufferMsgConsumerHandler
import ink.anur.io.common.handler.KanashiDecoder
import ink.anur.io.common.ShutDownHooker
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelPipeline
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2020/2/22
 *
 * 通用的 Server，提供了 ByteBuffer 的消费入口
 * 以及 pipeline 的定制入口
 */
class CoordinateServer(port: Int, shutDownHooker: ShutDownHooker,
                       private val howToConsumeByteBuffer: (ChannelHandlerContext, ByteBuffer) -> Unit,
                       private val howToConsumePipeline: (ChannelPipeline) -> Unit)
    : Server(port, shutDownHooker) {
    override fun channelPipelineConsumer(channelPipeline: ChannelPipeline): ChannelPipeline =
        channelPipeline.also { it.addLast(KanashiDecoder()).addLast(ByteBufferMsgConsumerHandler(howToConsumeByteBuffer)) }.also { howToConsumePipeline }
}