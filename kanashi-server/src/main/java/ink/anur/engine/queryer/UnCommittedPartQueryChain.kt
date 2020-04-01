package ink.anur.engine.queryer

import ink.anur.engine.memory.MemoryMVCCStorageUnCommittedPart
import ink.anur.engine.processor.EngineExecutor
import ink.anur.engine.queryer.common.QueryerChain
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.pojo.log.ByteBufferKanashiEntry
import ink.anur.pojo.log.KanashiCommand
import ink.anurengine.result.QueryerDefinition

/**
 * Created by Anur IjuoKaruKas on 2019/11/27
 *
 * 未提交部分的查询
 */
@NigateBean
class UnCommittedPartQueryChain : QueryerChain() {

    @NigateInject
    private lateinit var memoryMVCCStorageUnCommittedPart: MemoryMVCCStorageUnCommittedPart

    override fun doQuery(engineExecutor: EngineExecutor, useCurrencyRead: Boolean): ByteBufferKanashiEntry? {
        val dataHandler = engineExecutor.getDataHandler()

        val trxId = if (useCurrencyRead) {
            KanashiCommand.NON_TRX
        } else {
            dataHandler.getTrxId()
        }

        return memoryMVCCStorageUnCommittedPart.queryKeyInTrx(trxId, dataHandler.key)
    }
}