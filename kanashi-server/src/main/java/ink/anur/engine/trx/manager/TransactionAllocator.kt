package ink.anur.engine.trx.manager

import ink.anur.core.raft.ElectionMetaService
import ink.anur.core.raft.RaftCenterController
import ink.anur.exception.NotLeaderException
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject


/**
 * Created by Anur IjuoKaruKas on 2019/10/23
 *
 * 专门用于 leader 来生成事务id
 */
@NigateBean
class TransactionAllocator {

    @NigateInject
    private lateinit var metaService: ElectionMetaService

    companion object {
        const val StartTrx: Long = Long.MIN_VALUE
    }

    private var nowTrx: Long = StartTrx

    @Synchronized
    fun resetTrx(trx: Long) {
        nowTrx = trx
    }

    /**
     * 申请一个递增的事务id
     *
     * TODO 后续再考虑不够用的情况
     */
    @Synchronized
    internal fun allocate(): Long {
        if (!metaService.isLeader()) {
            throw NotLeaderException("非 leader 不可以申请事务id")
        }

        val trx = nowTrx
        if (trx == Long.MAX_VALUE) {
            nowTrx = Long.MIN_VALUE
        } else {
            nowTrx++
        }
        return trx
    }
}