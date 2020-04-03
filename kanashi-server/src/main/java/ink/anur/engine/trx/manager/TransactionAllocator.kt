package ink.anur.engine.trx.manager

import ink.anur.inject.NigateBean


/**
 * Created by Anur IjuoKaruKas on 2019/10/23
 *
 * 专门用于 leader 来生成事务id
 */
@NigateBean
class TransactionAllocator {

    companion object {
        const val StartTrx: Long = 0L
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
        val trx = nowTrx
        if (trx == Long.MAX_VALUE) {
            nowTrx = Long.MIN_VALUE
        } else {
            nowTrx++
        }
        return trx
    }
}