package ink.anur.io.client

import ink.anur.io.common.ShutDownHooker
import ink.anur.io.common.handler.AutoRegistryHandler
import ink.anur.io.common.handler.CoordinateDriverPoolHandler
import ink.anur.io.common.handler.ErrorHandler
import ink.anur.io.common.handler.KanashiDecoder
import io.netty.channel.ChannelPipeline

/**
 * Created by Anur IjuoKaruKas on 2020/2/23
 *
 * 可重连的协调 client
 */
class CoordinateClient(
    private val serverName: String,
    private val host: String,
    private val port: Int,
    private val shutDownHooker: ShutDownHooker) :
    ReConnectableClient(host, port, shutDownHooker) {

    override fun channelPipelineConsumer(channelPipeline: ChannelPipeline): ChannelPipeline {
        channelPipeline
            .addFirst(AutoRegistryHandler(serverName, host, port))
            .addLast(KanashiDecoder())
            .addLast(CoordinateDriverPoolHandler())
            .addLast(ErrorHandler())
        return channelPipeline
    }


    override fun howToRestart() {
        CoordinateClient(serverName, host, port, shutDownHooker).start()
    }
}