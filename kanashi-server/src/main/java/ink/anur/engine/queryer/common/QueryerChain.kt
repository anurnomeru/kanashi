package ink.anur.engine.queryer.common

import ink.anurengine.result.QueryerDefinition
import ink.anur.engine.processor.EngineExecutor
import ink.anur.pojo.log.ByteBufferKanashiEntry

/**
 * Created by Anur IjuoKaruKas on 2019/11/27
 *
 * 由于查询功能是由各个小模块提供的，所以使用责任链来实现
 */
abstract class QueryerChain {

    var next: QueryerChain? = null

    /**
     * 本层如何去执行查询
     */
    abstract fun doQuery(engineExecutor: EngineExecutor): ByteBufferKanashiEntry?

    /**
     * 如果到了最后一层都找不到，则返回此结果
     */
    private fun keyNotFoundTilEnd(engineExecutor: EngineExecutor) {
        engineExecutor.getEngineResult().setQueryExecutorDefinition(QueryerDefinition.TIL_END)
    }

    /**
     * shutSuccess 代表在查询到结果后，是否标记为已成功
     */
    fun query(engineExecutor: EngineExecutor): ByteBufferKanashiEntry? {
        // 优先执行 本层的 doQuery
        return doQuery(engineExecutor) ?: // 如果结果为空，则有下层执行下层，没有下层直接返回 null
        if (next != null) {
            next!!.query(engineExecutor)
        } else {
            keyNotFoundTilEnd(engineExecutor)
            null
        }
    }
}
