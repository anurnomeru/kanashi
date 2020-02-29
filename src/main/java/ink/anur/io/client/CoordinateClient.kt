package ink.anur.io.client

import ink.anur.io.common.ShutDownHooker
import ink.anur.io.common.handler.ByteBufferMsgConsumerHandler
import ink.anur.io.common.handler.ErrorHandler
import ink.anur.io.common.handler.KanashiDecoder
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelPipeline
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2020/2/23
 *
 * 可重连的 client
 */
class CoordinateClient(
    private val serverName: String,
    private val host: String,
    private val port: Int,
    private val shutDownHooker: ShutDownHooker,
    private val howToConsumeByteBuffer: (ChannelHandlerContext, ByteBuffer) -> Unit,
    private val howToConsumePipeline: (ChannelPipeline) -> Unit) :
    ReConnectableClient(serverName, host, port, shutDownHooker) {

    override fun channelPipelineConsumer(channelPipeline: ChannelPipeline): ChannelPipeline =
        channelPipeline.addLast(KanashiDecoder())
            .addLast(ByteBufferMsgConsumerHandler(howToConsumeByteBuffer))
            .addLast(ErrorHandler()).also(howToConsumePipeline)

    override fun howToRestart() {
        CoordinateClient(serverName, host, port, shutDownHooker, howToConsumeByteBuffer, howToConsumePipeline).start()
    }
}