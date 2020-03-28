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

    /**
     * 是否由客户端直接请求过来，如果是，要进行回复
     */
    var fromClient: String? = null

    /**
     * 消息时间，只是用于请求方辨别是哪个请求的回复用
     */
    var msgTime: Long = 0L

    /**
     * 整个操作是否完成的锁
     */
    val cdl = CountDownLatch(1)

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

    companion object {
        val logger = Debugger(this.javaClass).switch(DebuggerLevel.INFO)
    }
}
