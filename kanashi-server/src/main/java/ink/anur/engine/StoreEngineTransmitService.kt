package ink.anur.engine

import ink.anur.common.pool.EventDriverPool
import ink.anur.core.request.RequestProcessCentreService
import ink.anur.debug.Debugger
import ink.anur.engine.memory.MemoryMVCCStorageUnCommittedPart
import ink.anur.engine.processor.EngineExecutor
import ink.anur.engine.queryer.EngineDataQueryer
import ink.anur.engine.trx.lock.TrxFreeQueuedSynchronizer
import ink.anur.engine.trx.manager.TransactionManageService
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.pojo.command.KanashiCommandFail
import ink.anur.pojo.command.KanashiCommandResponse
import ink.anur.pojo.log.ByteBufferKanashiEntry
import ink.anur.pojo.log.KanashiCommand.Companion.NON_TRX
import ink.anur.pojo.log.common.CommandTypeEnum
import ink.anur.pojo.log.common.CommonApiTypeEnum
import ink.anur.pojo.log.common.StrApiTypeEnum
import java.util.concurrent.CountDownLatch
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

    @NigateInject
    private lateinit var requestProcessCentreService: RequestProcessCentreService

    init {
        /**
         * 用于返回客户端的结果
         */
        EventDriverPool.register(EngineExecutor::class.java, Runtime.getRuntime().availableProcessors() * 2, 20, TimeUnit.MILLISECONDS) {
            it.await()
            if (it.getDataHandler().shortTransaction) {
                doCommit(it.getDataHandler().getTrxId())
            }

            if (it.fromClient != null) {
                val engineResult = it.getEngineResult()
                if (engineResult.success) {
                    requestProcessCentreService.send(it.fromClient!!, KanashiCommandResponse(engineResult.getKanashiEntry()))
                } else {
                    requestProcessCentreService.send(it.fromClient!!, KanashiCommandFail())
                }
            }
        }
    }

    fun commandInvoke(engineExecutor: EngineExecutor) {
        val trxId = engineExecutor.getDataHandler().getTrxId()

        try {

            when (engineExecutor.getDataHandler().getCommandType()) {
                /*
                 * common 操作比较特殊，它直接会有些特殊交互，比如开启一个事务，关闭一个事务等。
                 */
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
                            engineDataQueryer.doQuery(engineExecutor) {
                                engineExecutor.getEngineResult().setKanashiEntry(it)
                                engineExecutor.shotSuccess()
                            }
                        }
                        StrApiTypeEnum.DELETE -> {
                            engineExecutor.getDataHandler().markKanashiEntryAsDeleteBeforeOperate()
                            doAcquire(engineExecutor)
                            engineExecutor.shotSuccess()
                        }
                        StrApiTypeEnum.SET -> {
                            doAcquire(engineExecutor)
                            engineExecutor.shotSuccess()
                        }
                        StrApiTypeEnum.SET_EXIST -> {
                            var result: ByteBufferKanashiEntry? = null
                            val cdl = CountDownLatch(1)
                            engineDataQueryer.doQuery(engineExecutor) {
                                result = it
                                cdl.countDown()
                            }
                            cdl.await()

                            if (result == null) {
                                engineExecutor.shotFailure()
                            } else {
                                doAcquire(engineExecutor)
                                engineExecutor.shotSuccess()
                            }
                        }
                        StrApiTypeEnum.SET_NOT_EXIST -> {
                            var result: ByteBufferKanashiEntry? = null
                            val cdl = CountDownLatch(1)
                            engineDataQueryer.doQuery(engineExecutor) {
                                result = it
                                cdl.countDown()
                            }
                            cdl.await()

                            if (result != null) {
                                engineExecutor.shotFailure()
                            } else {
                                doAcquire(engineExecutor)
                                engineExecutor.shotSuccess()
                            }
                        }
                        StrApiTypeEnum.SET_IF -> {

                            var result: ByteBufferKanashiEntry? = null
                            val cdl = CountDownLatch(1)
                            engineDataQueryer.doQuery(engineExecutor) {
                                result = it
                                cdl.countDown()
                            }
                            cdl.await()

                            val expectValue = engineExecutor.getDataHandler().extraParams[0]
                            if (result == null || result!!.getValueString() != expectValue) {
                                engineExecutor.shotFailure()
                            } else {
                                doAcquire(engineExecutor)
                                engineExecutor.shotSuccess()
                            }
                        }
                    }
                }
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
