package ink.anur.core.client

import ink.anur.common.KanashiRunnable
import ink.anur.common.Shutdownable
import ink.anur.common.struct.KanashiNode
import ink.anur.io.client.ReConnectableClient
import ink.anur.io.common.ShutDownHooker

/**
 * Created by Anur IjuoKaruKas on 2020/2/23
 *
 * 连接某客户端的 client
 */
class ClientOperateHandler(kanashiNode: KanashiNode) : KanashiRunnable(), Shutdownable {

    private val serverShutDownHooker = ShutDownHooker("终止与协调节点 $kanashiNode 的连接")

    private val coordinateClient = ReConnectableClient(kanashiNode.serverName, kanashiNode.host,
        kanashiNode.coordinatePort, this.serverShutDownHooker)

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