package ink.anur.io.server

import ink.anur.io.common.ShutDownHooker
import ink.anur.io.common.handler.CoordinateDriverPoolHandler
import ink.anur.io.common.handler.ErrorHandler
import ink.anur.io.common.handler.KanashiDecoder
import io.netty.channel.ChannelPipeline

/**
 * Created by Anur IjuoKaruKas on 2020/2/22
 *
 * 通用的 Server，提供了 ByteBuffer 的消费入口
 * 以及 pipeline 的定制入口
 */
class CoordinateServer(port: Int,
                       shutDownHooker: ShutDownHooker,
                       private val howToConsumePipeline: (ChannelPipeline) -> Unit)
    : Server(port, shutDownHooker) {
    override fun channelPipelineConsumer(channelPipeline: ChannelPipeline): ChannelPipeline {
        channelPipeline
            .addLast(KanashiDecoder())
            .addLast(CoordinateDriverPoolHandler())
            .addLast(ErrorHandler())
        howToConsumePipeline.invoke(channelPipeline)
        return channelPipeline
    }
}