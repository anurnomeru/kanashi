package ink.anur.io.client

import ink.anur.common.KanashiExecutors
import ink.anur.io.common.handler.ErrorHandler
import ink.anur.io.common.ShutDownHooker
import ink.anur.io.common.handler.ReconnectHandler
import io.netty.bootstrap.Bootstrap
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelPipeline
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import org.slf4j.LoggerFactory
import java.util.concurrent.CountDownLatch

/**
 * Created by Anur IjuoKaruKas on 2020/2/23
 *
 * 可重连的客户端
 */
abstract class ReConnectableClient(private val serverName: String, private val host: String, private val port: Int, private val shutDownHooker: ShutDownHooker) {

    private val logger = LoggerFactory.getLogger(this::class.java)

    private val reconnectLatch = CountDownLatch(1)

    /**
     * 暴露可定制的 channelPipelineConsumer
     */
    abstract fun channelPipelineConsumer(channelPipeline: ChannelPipeline): ChannelPipeline

    /**
     * 需要由子类来定制如何重启
     */
    abstract fun howToRestart()

    fun start() {

        val restartMission = Thread {
            try {
                reconnectLatch.await()
            } catch (e: InterruptedException) {
                e.printStackTrace()
            }

            if (shutDownHooker.isShutDown()) {
                logger.debug("与节点 {} [{}:{}] 的连接已被终止，无需再进行重连", serverName, host, port)
            } else {
                logger.trace("正在重新连接节点 {} [{}:{}] ...", serverName, host, port)
                this.howToRestart()
            }
        }
        KanashiExecutors.execute(restartMission)
        restartMission.name = "Client Restart [$serverName-$host:$port]"

        val group = NioEventLoopGroup()

        try {
            val bootstrap = Bootstrap()
            bootstrap.group(group)
                .channel(NioSocketChannel::class.java)
                .handler(object : ChannelInitializer<SocketChannel>() {

                    @Throws(Exception::class)
                    override fun initChannel(socketChannel: SocketChannel) {
                        channelPipelineConsumer(
                            socketChannel.pipeline()
                                .addLast(ReconnectHandler(serverName, reconnectLatch))) // 引入重连机制
                    }
                })

            val channelFuture = bootstrap.connect(host, port)
            channelFuture.addListener { future ->
                if (!future.isSuccess) {
                    if (reconnectLatch.count == 1L) {
                        logger.trace("连接节点 {} [{}:{}] 失败，准备进行重连 ...", serverName, host, port)
                    }

                    reconnectLatch.countDown()
                }
            }

            shutDownHooker.shutDownRegister { group.shutdownGracefully() }

            channelFuture.channel()
                .closeFuture()
                .sync()
        } catch (e: InterruptedException) {
            e.printStackTrace()
        } finally {
            try {
                group.shutdownGracefully()
                    .sync()
            } catch (e: InterruptedException) {
                e.printStackTrace()
            }
        }
    }
}