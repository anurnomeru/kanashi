package ink.anur.engine.trx.manager

import ink.anur.inject.NigateBean


/**
 * Created by Anur IjuoKaruKas on 2019/10/23
 *
 * 专门用于 leader 来生成事务id
 */
@NigateBean
class TrxAllocator {

    companion object {
        const val StartTrx: Long = 1
    }

    private var nowTrx: Long = StartTrx

    /**
     * 申请一个递增的事务id
     */
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