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

    private val CLIENT_PIPELINE_CONSUME: (ChannelPipeline) -> Unit = { it.addFirst(AutoRegistryHandler(kanashiNode)) }

    private val coordinateClient = CoordinateClient( kanashiNode.host,
        kanashiNode.coordinatePort, this.serverShutDownHooker, CLIENT_PIPELINE_CONSUME)

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