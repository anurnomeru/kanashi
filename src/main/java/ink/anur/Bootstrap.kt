package ink.anur

import ink.anur.core.server.CoordinateServerOperator

/**
 * Created by Anur IjuoKaruKas on 2020/2/22
 */
object Bootstrap {

    @Volatile
    private var RUNNING = true

    @JvmStatic
    fun main(args: Array<String>) {
        CoordinateServerOperator.start()

        var i = 0

        while (RUNNING) {
            i++
            Thread.sleep(1000)

            if (i == 10) {
                CoordinateServerOperator.shutDown()
            }
        }
    }
}