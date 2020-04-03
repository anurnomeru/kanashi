package ink.anur.engine.processor

import ink.anur.debug.Debugger
import ink.anur.debug.DebuggerLevel
import ink.anur.engine.result.EngineResult
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Created by Anur IjuoKaruKas on 2019/12/3
 *
 * 辅助访问访问数据层的媒介
 */
class EngineExecutor(private val dataHandler: DataHandler, val responseRegister: ResponseRegister? = null) {

    /**
     * 整个操作是否完成的锁
     */
    private val finishLatch = CountDownLatch(1)

    private val engineResult: EngineResult = EngineResult()

    /**
     * dataHandler 包含所有操作时需要的数据
     */
    fun getDataHandler(): DataHandler = dataHandler

    /**
     * 用于封装操作结果
     */
    fun getEngineResult() = engineResult

    fun await() {
        finishLatch.await()
    }

    fun await(time: Long, unit: TimeUnit): Boolean {
        return finishLatch.await(time, unit)
    }

    /**
     * 标记操作成功
     */
    fun shotSuccess() {
        finishLatch.countDown()
    }

    /**
     * 标记为失败
     */
    fun shotFailure() {
        finishLatch.countDown()
        engineResult.success = false
    }

    /**
     * 如果发生了错误
     */
    fun exceptionCaught(e: Throwable) {
        finishLatch.countDown()
        engineResult.err = e
        shotFailure()
    }

    companion object {
        val logger = Debugger(this.javaClass).switch(DebuggerLevel.INFO)
    }
}
