package ink.anur.core.server

import ink.anur.common.KanashiRunnable
import ink.anur.io.common.ShutDownHooker
import ink.anur.io.server.CoordinateServer
import org.slf4j.LoggerFactory

/**
 * Created by Anur IjuoKaruKas on 2020/2/22
 *
 * 协调服务器主逻辑入口
 */
object CoordinateServerOperator : KanashiRunnable() {

    private val logger = LoggerFactory.getLogger(CoordinateServerOperator::class.java)

    /**
     * 协调服务端
     */
    private var coordinateServer: CoordinateServer

    init {
        val sdh = ShutDownHooker("终止协调服务器的套接字接口 8080 的监听！")
        this.coordinateServer = CoordinateServer(8080,
            sdh, { _, _ -> }, {})
    }

    override fun run() {
        logger.info("协调服务器正在启动...")
        coordinateServer.start()
    }
}