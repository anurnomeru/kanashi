package ink.anur.core.client

import ink.anur.core.struct.KanashiNode
import ink.anur.inject.NigateBean
import org.slf4j.LoggerFactory

/**
 * Created by Anur IjuoKaruKas on 2020/2/24
 *
 * 负责管理 client 连接的服务
 */
@NigateBean
class CoordinateClientService {

    private val logger = LoggerFactory.getLogger(this::class.java)

    private val CONNECT_MAPPING: MutableMap<KanashiNode, CoordinateClientOperator> = mutableMapOf()

    @Synchronized
    fun getCoordinateClient(kanashiNode: KanashiNode): CoordinateClientOperator {
        var coordinateClientOperator = CONNECT_MAPPING[kanashiNode]
        if (coordinateClientOperator == null) {
            coordinateClientOperator = CoordinateClientOperator(kanashiNode)
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