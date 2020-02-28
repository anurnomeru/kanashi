package ink.anur.core.client

import ink.anur.common.KanashiRunnable
import ink.anur.common.Shutdownable
import ink.anur.common.pool.DriverPool
import ink.anur.struct.common.AbstractStruct
import ink.anur.struct.enumerate.OperationTypeEnum
import ink.anur.core.struct.CoordinateRequest
import ink.anur.core.struct.KanashiNode
import ink.anur.io.client.CoordinateClient
import ink.anur.io.common.ShutDownHooker
import ink.anur.io.common.handler.AutoRegistryHandler
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelPipeline
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2020/2/23
 *
 * 集群内通讯、协调服务器操作类客户端，负责协调相关的业务
 */
class CoordinateClientOperator(val kanashiNode: KanashiNode) : KanashiRunnable(), Shutdownable {

    private val serverShutDownHooker = ShutDownHooker("终止与协调节点 $kanashiNode 的连接")

    /**
     * CoordinateServerOperator 消费逻辑
     * 这里直接将解码后的 msg 丢入 HandlerPool
     */
    private val CLIENT_MSG_CONSUMER: (ChannelHandlerContext, ByteBuffer) -> Unit = { ctx, msg ->
        var sign = 0
        try {
            sign = msg.getInt(AbstractStruct.TypeOffset)
        } catch (e: Exception) {
            e.printStackTrace()
        }

        val typeEnum = OperationTypeEnum.parseByByteSign(sign)
        DriverPool.offer(CoordinateRequest(msg, typeEnum, ctx.channel()))
    }

    private val CLIENT_PIPELINE_CONSUME: (ChannelPipeline) -> Unit = { it.addFirst(AutoRegistryHandler(kanashiNode)) }

    private val coordinateClient = CoordinateClient(kanashiNode.serverName, kanashiNode.host,
        kanashiNode.coordinatePort, this.serverShutDownHooker, CLIENT_MSG_CONSUMER, CLIENT_PIPELINE_CONSUME)

    override fun run() {
        if (serverShutDownHooker.isShutDown()) {
            println("zzzzzzzz??????????????zzzzzzzzzzzzzzzzzzzzzzzz")
            println("zzzzzzzz??????????????zzzzzzzzzzzzzzzzzzzzzzzz")
            println("zzzzzzzz??????????????zzzzzzzzzzzzzzzzzzzzzzzz")
            println("zzzzzzzz??????????????zzzzzzzzzzzzzzzzzzzzzzzz")
            println("zzzzzzzz??????????????zzzzzzzzzzzzzzzzzzzzzzzz")
        }
        coordinateClient.start()
    }

    override fun shutDown() {
        serverShutDownHooker.shutdown()
    }
}