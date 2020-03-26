package ink.anur.engine

import ink.anur.debug.Debugger
import ink.anur.engine.memory.MemoryMVCCStorageUnCommittedPart
import ink.anur.engine.processor.DataHandler
import ink.anur.engine.processor.EngineExecutor
import ink.anur.engine.queryer.EngineDataQueryer
import ink.anur.engine.result.EngineResult
import ink.anur.engine.trx.lock.TrxFreeQueuedSynchronizer
import ink.anur.engine.trx.manager.TransactionManageService
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.pojo.log.ByteBufferKanashiEntry
import ink.anur.pojo.log.base.LogItem
import ink.anur.pojo.log.common.CommandTypeEnum
import ink.anur.pojo.log.common.CommonApiTypeEnum
import ink.anur.pojo.log.common.StrApiTypeEnum


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

    // 返回值仅用于测试校验
    fun commandInvoke(logItem: LogItem, engineExecutor: EngineExecutor = EngineExecutor()) {
        val dataHandler = DataHandler(logItem)
        engineExecutor.setDataHandler(dataHandler)

        var trxId = dataHandler.getTrxId()

        try {
            var selectOperate = false

            /*
             * common 操作比较特殊，它直接会有些特殊交互，比如开启一个事务，关闭一个事务等。
             */
            when (dataHandler.getCommandType()) {
                CommandTypeEnum.COMMON -> {
                    when (dataHandler.getApi()) {
                        CommonApiTypeEnum.START_TRX -> {
//                            logger.trace("事务 [{}] 已经开启", trxId)
                            return
                        }
                        CommonApiTypeEnum.COMMIT_TRX -> {
                            doCommit(trxId)
                            return
                        }
                        CommonApiTypeEnum.ROLL_BACK -> {
//                            throw RollbackException() todo 还没写
                        }
                    }
                }

                CommandTypeEnum.STR -> {
                    when (dataHandler.getApi()) {
                        StrApiTypeEnum.SELECT -> {
                            selectOperate = true
                            engineDataQueryer.doQuery(engineExecutor)
                        }
                        StrApiTypeEnum.DELETE -> {
                            doAcquire(engineExecutor, ByteBufferKanashiEntry.Companion.OperateType.DISABLE)
                        }
                        StrApiTypeEnum.SET -> {
                            doAcquire(engineExecutor, ByteBufferKanashiEntry.Companion.OperateType.ENABLE)
                        }
                        StrApiTypeEnum.SET_EXIST -> {
                            engineDataQueryer.doQuery(engineExecutor)
                            engineExecutor.kanashiEntry()
                                ?.also { engineExecutor.shotFailure() }
                                ?: also {
                                    doAcquire(engineExecutor, ByteBufferKanashiEntry.Companion.OperateType.ENABLE)
                                }
                        }
                        StrApiTypeEnum.SET_NOT_EXIST -> {
                            engineDataQueryer.doQuery(engineExecutor)
                            engineExecutor.kanashiEntry()
                                ?.also {
                                    doAcquire(engineExecutor, ByteBufferKanashiEntry.Companion.OperateType.ENABLE)
                                }
                                ?: also { engineExecutor.shotFailure() }
                        }
                        StrApiTypeEnum.SET_IF -> {
                            engineDataQueryer.doQuery(engineExecutor)
                            val currentValue = engineExecutor.kanashiEntry()?.getValue()
                            val expectValue = dataHandler.extraParams[0]

                            if (expectValue == currentValue) {
                                doAcquire(engineExecutor, ByteBufferKanashiEntry.Companion.OperateType.ENABLE)
                            } else {
                                engineExecutor.shotFailure()
                            }
                        }
                    }
                }
            }

            if (dataHandler.shortTransaction && !selectOperate) doCommit(trxId)
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
    private fun doAcquire(engineExecutor: EngineExecutor, operateType: ByteBufferKanashiEntry.Companion.OperateType) {
        val dataHandler = engineExecutor.getDataHandler()
        val trxId = dataHandler.getTrxId()

        dataHandler.setOperateType(operateType)
        trxFreeQueuedSynchronizer.acquire(trxId, dataHandler.key) {
            memoryMVCCStorageUnCommittedPart.commonOperate(dataHandler)
        }
//        logger.trace("事务 [{}] 将 key [{}] 设置为了新值", trxId, engineExecutor.getDataHandler().key)
    }

    /**
     * 进行事务控制与数据流转，
     * 1、首先要从无锁控制释放该锁，我们的很多操作都是经由 TrxFreeQueuedSynchronizer 进行控制锁并发的
     * 2、将数据推入 commitPart
     * 3、通知事务控制器，事务已经被销毁
     */
    private fun doCommit(trxId: Long) {
        trxFreeQueuedSynchronizer.release(trxId) { keys ->
            keys?.let { memoryMVCCStorageUnCommittedPart.flushToCommittedPart(trxId, it) }
            transactionManageService.releaseTrx(trxId)
        }

//        logger.trace("事务 [{}] 已经提交", trxId)
    }

    private fun doRollBack(trxId: Long) {
        // todo 还没写 懒得写
    }
}
