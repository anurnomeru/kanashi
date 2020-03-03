//package ink.anur.io.client
//
//import ink.anur.io.common.ShutDownHooker
//import ink.anur.io.common.handler.CoordinateDriverPoolHandler
//import ink.anur.io.common.handler.ErrorHandler
//import ink.anur.io.common.handler.KanashiDecoder
//import io.netty.channel.ChannelPipeline
//
///**
// * Created by Anur IjuoKaruKas on 2020/3/3
// */
//class ServiceClient(
//    private val host: String,
//    private val port: Int,
//    private val shutDownHooker: ShutDownHooker) :
//    ReConnectableClient(host, port, shutDownHooker) {
//
//    override fun channelPipelineConsumer(channelPipeline: ChannelPipeline): ChannelPipeline {
//        channelPipeline
//            .addLast(KanashiDecoder())
//            .addLast(CoordinateDriverPoolHandler())
//            .addLast(ErrorHandler())
//        howToConsumePipeline.invoke(channelPipeline)
//        return channelPipeline
//    }
//
//
//    override fun howToRestart() {
//        CoordinateClient(host, port, shutDownHooker, howToConsumePipeline).start()
//    }
//}