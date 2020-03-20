package ink.anur.engine.processor

import ink.anur.debug.Debugger
import ink.anur.debug.DebuggerLevel
import ink.anur.engine.result.EngineResult
import ink.anur.pojo.log.ByteBufferKanashiEntry
import java.rmi.UnexpectedException

/**
 * Created by Anur IjuoKaruKas on 2019/12/3
 *
 * 辅助访问访问数据层的媒介
 */
class EngineExecutor(val engineResult: EngineResult) {

    companion object {
        val logger = Debugger(this.javaClass).switch(DebuggerLevel.INFO)
    }

    private var dataHandler: DataHandler? = null

    fun kanashiEntry(): ByteBufferKanashiEntry? = engineResult.getKanashiEntry()

    fun setDataHandler(dataHandler: DataHandler) {
        this.dataHandler = dataHandler
    }

    fun getDataHandler(): DataHandler = dataHandler ?: throw UnexpectedException("参数没有设置？？？？")

    /**
     * 标记为失败
     */
    fun shotFailure() {
        engineResult.result = false
    }

    /**
     * 如果发生了错误
     */
    fun exceptionCaught(e: Throwable) {
        engineResult.err = e
        engineResult.result = false
    }
}