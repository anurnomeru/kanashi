package ink.anur.engine.queryer.common

import ink.anurengine.result.QueryerDefinition
import ink.anur.engine.processor.EngineExecutor

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
    abstract fun doQuery(engineExecutor: EngineExecutor)

    /**
     * 如果到了最后一层都找不到，则返回此结果
     */
    private fun keyNotFoundTilEnd(engineExecutor: EngineExecutor) {
        engineExecutor.getEngineResult().setQueryExecutorDefinition(QueryerDefinition.TIL_END)
    }

    fun query(engineExecutor: EngineExecutor) {
        // 优先执行 本层的 doQuery
        doQuery(engineExecutor)
            // 如果拿到结果则返回结果
            .let { engineExecutor.getEngineResult().getKanashiEntry() }

        // 否则执行下一层
            ?: next?.query(engineExecutor).let { engineExecutor.getEngineResult().getKanashiEntry() }

            // 最后执行兜底
            ?: keyNotFoundTilEnd(engineExecutor)
    }
}
