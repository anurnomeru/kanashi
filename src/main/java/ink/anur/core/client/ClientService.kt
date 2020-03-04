package ink.anur.core.client

import ink.anur.common.struct.KanashiNode
import ink.anur.inject.NigateBean
import org.slf4j.LoggerFactory

/**
 * Created by Anur IjuoKaruKas on 2020/2/24
 *
 * 负责管理 client 连接的服务
 */
@NigateBean
class ClientService {

    private val logger = LoggerFactory.getLogger(this::class.java)

    private val CONNECT_MAPPING: MutableMap<KanashiNode, ClientOperateHandler> = mutableMapOf()

    @Synchronized
    fun connect(kanashiNode: KanashiNode): ClientOperateHandler {
        var coordinateClientOperator = CONNECT_MAPPING[kanashiNode]
        if (coordinateClientOperator == null) {
            logger.info("正在发起与协调节点 $kanashiNode 的连接...")
            coordinateClientOperator = ClientOperateHandler(kanashiNode)
            CONNECT_MAPPING[kanashiNode] = coordinateClientOperator
            coordinateClientOperator.start()
        }
        return coordinateClientOperator
    }

    @Synchronized
    fun shutDownCoordinateClient(kanashiNode: KanashiNode) {
        CONNECT_MAPPING[kanashiNode]?.shutDown()
    }
}