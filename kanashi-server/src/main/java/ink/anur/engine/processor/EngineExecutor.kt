package ink.anur.engine.processor

import ink.anur.debug.Debugger
import ink.anur.debug.DebuggerLevel
import ink.anur.engine.result.EngineResult
import ink.anur.pojo.log.ByteBufferKanashiEntry

/**
 * Created by Anur IjuoKaruKas on 2019/12/3
 *
 * 辅助访问访问数据层的媒介
 */
class EngineExecutor(private val dataHandler: DataHandler) {

    var fromClient: String? = null

    companion object {
        val logger = Debugger(this.javaClass).switch(DebuggerLevel.INFO)
    }

    private val engineResult: EngineResult = EngineResult()

    fun kanashiEntry(): ByteBufferKanashiEntry? = engineResult.getKanashiEntry()

    fun getDataHandler(): DataHandler = dataHandler

    fun getEngineResult() = engineResult

    /**
     * 标记为失败
     */
    fun shotFailure() {
        engineResult.success = false
    }

    /**
     * 如果发生了错误
     */
    fun exceptionCaught(e: Throwable) {
        engineResult.err = e
        engineResult.success = false
    }
}