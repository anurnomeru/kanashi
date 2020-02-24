package ink.anur.core.client

import ink.anur.core.struct.KanashiNode
import ink.anur.inject.NigateBean
import org.slf4j.LoggerFactory

/**
 * Created by Anur IjuoKaruKas on 2020/2/24
 *
 * 负责管理 client 连接的服务
 *
 * 因为同时只能连接一个leader，所以需要根据 kanashiNode 来获取一个连接，
 * 如果连接已经存在则直接获取
 *
 * 如果不存在则关闭现有链接，然后发起新的TCP连接
 */
@NigateBean
class CoordinateClientService {

    private val logger = LoggerFactory.getLogger(this::class.java)

    /**
     * 连接Leader节点的协调器连接，只能同时存在一个，如果要连接新的Leader，则需要将旧节点的连接关闭
     */
    @Volatile
    private var INSTANCE: CoordinateClientOperator? = null

    /**
     * 连接Leader节点的协调器连接，只能同时存在一个，如果要连接新的Leader，则需要将旧节点的连接关闭
     */
    fun getCoordinateClient(kanashiNode: KanashiNode): CoordinateClientOperator {
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