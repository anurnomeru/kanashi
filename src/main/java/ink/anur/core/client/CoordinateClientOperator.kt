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
class CoordinateClientOperator(private val kanashiNode: KanashiNode) : KanashiRunnable(), Shutdownable {

    companion object {
        private val logger = LoggerFactory.getLogger(this::class.java)

        /**
         * 连接Leader节点的协调器连接，只能同时存在一个，如果要连接新的Leader，则需要将旧节点的连接关闭
         */
        @Volatile
        private var INSTANCE: CoordinateClientOperator? = null

        /**
         * 连接Leader节点的协调器连接，只能同时存在一个，如果要连接新的Leader，则需要将旧节点的连接关闭
         */
        fun getInstance(kanashiNode: KanashiNode): CoordinateClientOperator {
            if (kanashiNode != INSTANCE?.kanashiNode) {
                synchronized(CoordinateClientOperator::class.java) {
                    if (kanashiNode != INSTANCE?.kanashiNode) {
                        INSTANCE?.shutDown()

                        INSTANCE = CoordinateClientOperator(kanashiNode)
                        INSTANCE?.name = "CoordinateClientOperator - [$kanashiNode]"
                        INSTANCE?.start()
                    }
                }
            }
            return INSTANCE!!
        }
    }


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