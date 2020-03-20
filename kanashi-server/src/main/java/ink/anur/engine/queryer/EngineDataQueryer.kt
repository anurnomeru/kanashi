package ink.anur.engine.queryer

import ink.anur.engine.processor.EngineExecutor
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import javax.annotation.PostConstruct

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

    @PostConstruct
    private fun init() {
        ucChain.next = cChain
        cChain.next = mChain
    }

    fun doQuery(engineExecutor: EngineExecutor) = ucChain.query(engineExecutor)
}