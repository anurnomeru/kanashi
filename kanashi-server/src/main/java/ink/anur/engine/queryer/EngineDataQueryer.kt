package ink.anur.engine.queryer

import ink.anur.common.KanashiExecutors
import ink.anur.engine.processor.EngineExecutor
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.inject.NigatePostConstruct
import ink.anur.pojo.log.ByteBufferKanashiEntry

/**
 * Created by Anur IjuoKaruKas on 2019/10/31
 *
 * 专用于数据查询
 */
@NigateBean
class EngineDataQueryer {

    @NigateInject
    private lateinit var ucChain: UnCommittedPartQueryChain

    @NigateInject
    private lateinit var cChain: CommittedPartQueryChain

    @NigateInject
    private lateinit var mChain: MemoryLSMQueryChain

    @NigatePostConstruct
    private fun init() {
        ucChain.next = cChain
        cChain.next = mChain
    }

    fun doQuery(engineExecutor: EngineExecutor, afterQuery: (ByteBufferKanashiEntry?) -> Unit) =
        KanashiExecutors.execute(Runnable {
            val query = ucChain.query(engineExecutor)
            afterQuery.invoke(query)
        })
}