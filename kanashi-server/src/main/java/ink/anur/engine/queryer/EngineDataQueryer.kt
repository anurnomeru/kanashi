package ink.anur.engine.queryer

import ink.anur.common.KanashiExecutors
import ink.anur.common.pool.EventDriverPool
import ink.anur.engine.processor.EngineExecutor
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import java.util.concurrent.TimeUnit
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

    fun doQuery(engineExecutor: EngineExecutor) = KanashiExecutors.execute(Runnable { ucChain.query(engineExecutor) })
}