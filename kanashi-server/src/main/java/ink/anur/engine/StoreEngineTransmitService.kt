package ink.anur.engine

import ink.anur.common.pool.EventDriverPool
import ink.anur.debug.Debugger
import ink.anur.engine.memory.MemoryMVCCStorageUnCommittedPart
import ink.anur.engine.processor.EngineExecutor
import ink.anur.engine.queryer.EngineDataQueryer
import ink.anur.engine.trx.lock.TrxFreeQueuedSynchronizer
import ink.anur.engine.trx.manager.TransactionManageService
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.pojo.log.KanashiCommand.Companion.NON_TRX
import ink.anur.pojo.log.common.CommandTypeEnum
import ink.anur.pojo.log.common.CommonApiTypeEnum
import ink.anur.pojo.log.common.StrApiTypeEnum
import java.util.concurrent.TimeUnit


/**
 * Created by anur. IjuoKaruKas on 2019/10/24
 *
 * 数据流转控制
 */
@NigateBean
class StoreEngineTransmitService {

    private val logger = Debugger(this.javaClass)

    @NigateInject
    private lateinit var engineDataQueryer: EngineDataQueryer

    @NigateInject
    private lateinit var trxFreeQueuedSynchronizer: TrxFreeQueuedSynchronizer

    @NigateInject
    private lateinit var transactionManageService: TransactionManageService

    @NigateInject
    private lateinit var memoryMVCCStorageUnCommittedPart: MemoryMVCCStorageUnCommittedPart

    init {
        EventDriverPool.register(EngineExecutor::class.java, Runtime.getRuntime().availableProcessors() * 2, 20, TimeUnit.MILLISECONDS) {
            if (it.fromClient != null) {
                it.await()
                val engineResult = it.getEngineResult()
                if (engineResult.success) {
                    val kanashiEntry = engineResult.getKanashiEntry()

                }
            }
        }
    }

    fun commandInvoke(engineExecutor: EngineExecutor) {
        val trxId = engineExecutor.getDataHandler().getTrxId()

        try {
            /*
             * common 操作比较特殊，它直接会有些特殊交互，比如开启一个事务，关闭一个事务等。
             */
            when (engineExecutor.getDataHandler().getCommandType()) {
                CommandTypeEnum.COMMON -> {
                    when (engineExecutor.getDataHandler().getApi()) {
                        CommonApiTypeEnum.START_TRX -> {
                            engineExecutor.shotSuccess()
                        }
                        CommonApiTypeEnum.COMMIT_TRX -> {
                            doCommit(trxId)
                            engineExecutor.shotSuccess()
                        }
                        CommonApiTypeEnum.ROLL_BACK -> {
//                            throw RollbackException() todo 还没写
                        }
                    }
                }

                CommandTypeEnum.STR -> {
                    when (engineExecutor.getDataHandler().getApi()) {
                        StrApiTypeEnum.SELECT -> {
                            engineDataQueryer.doQuery(engineExecutor)
                        }
                        StrApiTypeEnum.DELETE -> {
                            engineExecutor.getDataHandler().markKanashiEntryAsDeleteBeforeOperate()
                            doAcquire(engineExecutor)
                        }
                        StrApiTypeEnum.SET -> {
                            doAcquire(engineExecutor)
                        }
                        StrApiTypeEnum.SET_EXIST -> {
                            engineDataQueryer.doQuery(engineExecutor)
                            engineExecutor.kanashiEntry()
                                ?.also { engineExecutor.shotFailure() }
                                ?: also {
                                    doAcquire(engineExecutor)
                                }
                        }
                        StrApiTypeEnum.SET_NOT_EXIST -> {
                            engineDataQueryer.doQuery(engineExecutor)
                            engineExecutor.kanashiEntry()
                                ?.also {
                                    doAcquire(engineExecutor)
                                }
                                ?: also { engineExecutor.shotFailure() }
                        }
                        StrApiTypeEnum.SET_IF -> {
                            engineDataQueryer.doQuery(engineExecutor)
                            val currentValue = engineExecutor.kanashiEntry()?.getValueString()
                            val expectValue = engineExecutor.getDataHandler().extraParams[0]

                            if (expectValue == currentValue) {
                                doAcquire(engineExecutor)
                            } else {
                                engineExecutor.shotFailure()
                            }
                        }
                    }
                }
            }

            if (engineExecutor.getDataHandler().shortTransaction) {
                engineExecutor.await()// 必须等待操作完才能 commit
                doCommit(trxId)
            }
        } catch (e: Throwable) {
            logger.error("存储引擎执行出错，将执行回滚，原因 [{}]", e.message)
            e.printStackTrace()

            doRollBack(trxId)
            engineExecutor.exceptionCaught(e)
        }
    }

    /**
     * 进行事务控制与数据流转，通过无锁控制来进行阻塞与唤醒
     *
     * 如果拿到锁，则调用api，将数据插入 未提交部分(uc)
     */
    private fun doAcquire(engineExecutor: EngineExecutor) {
        val dataHandler = engineExecutor.getDataHandler()
        val trxId = dataHandler.getTrxId()

        trxFreeQueuedSynchronizer.acquire(trxId, dataHandler.key) {
            memoryMVCCStorageUnCommittedPart.commonOperate(dataHandler)
        }
        engineExecutor.shotSuccess()
    }

    /**
     * 进行事务控制与数据流转，
     * 1、首先要从无锁控制释放该锁，我们的很多操作都是经由 TrxFreeQueuedSynchronizer 进行控制锁并发的
     * 2、将数据推入 commitPart
     * 3、通知事务控制器，事务已经被销毁
     */
    private fun doCommit(trxId: Long) {
        if (trxId == NON_TRX) {
            return
        }

        trxFreeQueuedSynchronizer.release(trxId) { keys ->
            keys?.let { memoryMVCCStorageUnCommittedPart.flushToCommittedPart(trxId, it) }
            transactionManageService.releaseTrx(trxId)
        }
    }

    private fun doRollBack(trxId: Long) {
        // todo 还没写 懒得写
    }
}
