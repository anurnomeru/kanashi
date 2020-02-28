package ink.anur

import ink.anur.config.BootstrapConfiguration
import ink.anur.core.server.CoordinateServerOperatorService
import ink.anur.inject.Nigate

/**
 * Created by Anur IjuoKaruKas on 2020/2/22
 */
object Bootstrap {

    @Volatile
    private var RUNNING = true

    @JvmStatic
    fun main(args: Array<String>) {
        // 保存jvm参数
        BootstrapConfiguration.init(args)
        // 初始化 bean管理
        Nigate


        var i = 0

        while (RUNNING) {
            i++
            Thread.sleep(1000)
        }
    }
}