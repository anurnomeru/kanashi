package ink.anur.engine.queryer

import ink.anur.engine.memory.MemoryMVCCStorageCommittedPart
import ink.anurengine.result.QueryerDefinition
import ink.anur.engine.processor.EngineExecutor
import ink.anur.engine.queryer.common.QueryerChain
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject

/**
 * Created by Anur IjuoKaruKas on 2019/11/27
 *
 * 未提交部分的查询
 *
 *     这个是最特殊的，因为已提交部分的隔离性控制比较复杂
 *             涉及到事务创建时，
 */
@NigateBean
class CommittedPartQueryChain : QueryerChain() {

    @NigateInject
    private lateinit var memoryMVCCStorageCommittedPart:MemoryMVCCStorageCommittedPart

    override fun doQuery(engineExecutor: EngineExecutor) {
        val dataHandler = engineExecutor.getDataHandler()
        memoryMVCCStorageCommittedPart.queryKeyInTrx(dataHandler.getTrxId(), dataHandler.key, dataHandler.waterMarker)
                ?.also {
                    engineExecutor.engineResult.setKanashiEntry(it)
                    engineExecutor.engineResult.queryExecutorDefinition = QueryerDefinition.COMMIT_PART
                }
    }
}