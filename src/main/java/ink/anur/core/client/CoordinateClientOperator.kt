package ink.anur.core.client

import ink.anur.common.KanashiRunnable
import ink.anur.common.Shutdownable
import ink.anur.core.struct.KanashiNode
import ink.anur.io.client.CoordinateClient
import ink.anur.io.common.ShutDownHooker
import org.slf4j.LoggerFactory

/**
 * Created by Anur IjuoKaruKas on 2020/2/23
 *
 * 集群内通讯、协调服务器操作类客户端，负责协调相关的业务
 */
class CoordinateClientOperator(val kanashiNode: KanashiNode) : KanashiRunnable(), Shutdownable {

    private val serverShutDownHooker = ShutDownHooker("终止与协调节点 $kanashiNode 的连接")

    private val coordinateClient = CoordinateClient(kanashiNode.serverName, kanashiNode.host,
        kanashiNode.coordinatePort, this.serverShutDownHooker, { _, _ -> }, {})

    override fun run() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun shutDown() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}