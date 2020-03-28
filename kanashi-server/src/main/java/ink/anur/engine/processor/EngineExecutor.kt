package ink.anur.engine.processor

import ink.anur.debug.Debugger
import ink.anur.debug.DebuggerLevel
import ink.anur.engine.result.EngineResult
import ink.anur.pojo.log.ByteBufferKanashiEntry
import ink.anurengine.result.QueryerDefinition
import java.util.concurrent.CountDownLatch

/**
 * Created by Anur IjuoKaruKas on 2019/12/3
 *
 * 辅助访问访问数据层的媒介
 */
class EngineExecutor(private val dataHandler: DataHandler) {

    var fromClient: String? = null

    val cdl = CountDownLatch(1)

    companion object {
        val logger = Debugger(this.javaClass).switch(DebuggerLevel.INFO)
    }

    private val engineResult: EngineResult = EngineResult()

    fun kanashiEntry(): ByteBufferKanashiEntry? = engineResult.getKanashiEntry()

    fun getDataHandler(): DataHandler = dataHandler

    fun getEngineResult() = engineResult

    fun await() {
        cdl.await()
    }

    /**
     * 标记操作成功
     */
    fun shotSuccess() {
        cdl.countDown()
    }

    /**
     * 标记为失败
     */
    fun shotFailure() {
        cdl.countDown()
        engineResult.success = false
    }

    /**
     * 如果发生了错误
     */
    fun exceptionCaught(e: Throwable) {
        cdl.countDown()
        engineResult.err = e
        shotFailure()
    }
}