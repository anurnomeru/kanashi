package ink.anur.core.client

import ink.anur.common.KanashiRunnable
import ink.anur.io.client.CoordinateClient
import org.slf4j.LoggerFactory

/**
 * Created by Anur IjuoKaruKas on 2020/2/23
 *
 * 集群内通讯、协调服务器操作类客户端，负责协调相关的业务
 */
class CoordinateClientOperator : KanashiRunnable() {

    companion object {
        private val logger = LoggerFactory.getLogger(this::class.java)
    }

    /**
     * 连接Leader节点的协调器连接，只能同时存在一个，如果要连接新的Leader，则需要将旧节点的连接关闭
     */
    private var coordinateClient: CoordinateClient? = null


    override fun run() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}