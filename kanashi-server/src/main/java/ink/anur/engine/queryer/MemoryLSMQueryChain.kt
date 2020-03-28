package ink.anur.engine.queryer

import ink.anur.engine.memory.MemoryLSMService
import ink.anur.engine.processor.EngineExecutor
import ink.anur.engine.queryer.common.QueryerChain
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anurengine.result.QueryerDefinition

/**
 * Created by Anur IjuoKaruKas on 2019/11/27
 *
 * 未提交部分的查询
 */
@NigateBean
class MemoryLSMQueryChain : QueryerChain() {

    @NigateInject
    private lateinit var memoryLSMService: MemoryLSMService

    override fun doQuery(engineExecutor: EngineExecutor) {
        val dataHandler = engineExecutor.getDataHandler()
        memoryLSMService.get(dataHandler.key)
            ?.also {
                engineExecutor.getEngineResult().setKanashiEntry(it)
                engineExecutor.getEngineResult().setQueryExecutorDefinition(QueryerDefinition.MEMORY_LSM)
                engineExecutor.shotSuccess()
            }
    }
}