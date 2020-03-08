package ink.anur.io.client

import ink.anur.common.KanashiExecutors
import ink.anur.common.struct.KanashiNode
import ink.anur.io.common.ShutDownHooker
import ink.anur.io.common.handler.AutoRegistryHandler
import ink.anur.io.common.handler.EventDriverPoolHandler
import ink.anur.io.common.handler.ErrorHandler
import ink.anur.io.common.handler.KanashiDecoder
import ink.anur.io.common.handler.ReconnectHandler
import io.netty.bootstrap.Bootstrap
import io.netty.channel.ChannelInitializer
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
class ReConnectableClient(private val node: KanashiNode, private val shutDownHooker: ShutDownHooker) {

    private val logger = LoggerFactory.getLogger(this::class.java)

    private val reconnectLatch = CountDownLatch(1)

    fun start() {

        val restartMission = Thread {
            try {
                reconnectLatch.await()
            } catch (e: InterruptedException) {
                e.printStackTrace()
            }

            if (shutDownHooker.isShutDown()) {
                logger.debug("与节点 $node 的连接已被终止，无需再进行重连")
            } else {
                logger.trace("正在重新连接节点 $node ...")
                ReConnectableClient(node, shutDownHooker).start()
            }
        }
        KanashiExecutors.execute(restartMission)
        restartMission.name = "Client Restart $node"

        val group = NioEventLoopGroup()

        try {
            val bootstrap = Bootstrap()
            bootstrap.group(group)
                .channel(NioSocketChannel::class.java)
                .handler(object : ChannelInitializer<SocketChannel>() {

                    @Throws(Exception::class)
                    override fun initChannel(socketChannel: SocketChannel) {
                        socketChannel.pipeline()
                            .addFirst(AutoRegistryHandler(node)) // 自动注册到管道管理服务
                            .addLast(KanashiDecoder())// 解码处理器
                            .addLast(EventDriverPoolHandler())// 消息事件驱动
                            .addLast(ReconnectHandler(reconnectLatch))// 重连控制器
                            .addLast(ErrorHandler())// 错误处理
                    }
                })

            val channelFuture = bootstrap.connect(node.host, node.port)
            channelFuture.addListener { future ->
                if (!future.isSuccess) {
                    if (reconnectLatch.count == 1L) {
                        logger.trace("连接节点 $node 失败，准备进行重连 ...")
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