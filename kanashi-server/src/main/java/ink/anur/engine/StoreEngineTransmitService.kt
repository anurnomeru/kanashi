package ink.anur.engine

import ink.anur.common.pool.EventDriverPool
import ink.anur.core.request.MsgProcessCentreService
import ink.anur.debug.Debugger
import ink.anur.engine.memory.MemoryMVCCStorageUnCommittedPart
import ink.anur.engine.processor.EngineExecutor
import ink.anur.engine.processor.ResponseRegister
import ink.anur.engine.queryer.EngineDataQueryer
import ink.anur.engine.trx.lock.TrxFreeQueuedSynchronizer
import ink.anur.engine.trx.manager.TransactionManageService
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.pojo.command.KanashiCommandResponse
import ink.anur.pojo.log.ByteBufferKanashiEntry
import ink.anur.pojo.log.KanashiCommand.Companion.NON_TRX
import ink.anur.pojo.log.common.CommandTypeEnum
import ink.anur.pojo.log.common.CommonApiTypeEnum
import ink.anur.pojo.log.common.GenerationAndOffset
import ink.anur.pojo.log.common.StrApiTypeEnum
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
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
    private lateinit var msgProcessCentreService: MsgProcessCentreService

    /**
     * 一切未提交的事务都在这里有一个存档
     */
    private val unCommitGao = ConcurrentHashMap<Long, Boolean>()

    init {
        /**
         * 用于返回客户端的结果
         */
        EventDriverPool.register(EngineExecutor::class.java, 2, 200, TimeUnit.MILLISECONDS) {
            if (!it.await(2, TimeUnit.SECONDS)) {
                println("阻塞过久~~")
            }
            if (it.getDataHandler().shortTransaction) {
                doCommit(it.getDataHandler().getTrxId())
            }

            /**
             * 这是普通的查询回复，直接回复即可
             */
            if (it.responseRegister != null) {
                val responseRegister = it.responseRegister
                val engineResult = it.getEngineResult()
                msgProcessCentreService.send(responseRegister.fromClient, KanashiCommandResponse(responseRegister.msgTime, engineResult.success, engineResult.getKanashiEntry()))
            }

            val gao = it.getDataHandler().gao

            /**
             * 对于 leader 这是非查询操作的回复
             */
            if (gao != null && responseMap[gao] != null) {
                val responseRegister = responseMap[gao]!!
                val engineResult = it.getEngineResult()
                msgProcessCentreService.send(responseRegister.fromClient, KanashiCommandResponse(responseRegister.msgTime, engineResult.success, engineResult.getKanashiEntry()))

                responseMap.remove(gao)
            }
        }
    }

    private val responseMap = ConcurrentHashMap<GenerationAndOffset, ResponseRegister>()

    /**
     * 由于提交给 leader 的指令须要等半数提交才能回复
     * 所以暂存一份待回复信息在此
     *
     */
    fun waitForResponse(gao: GenerationAndOffset, responseRegister: ResponseRegister) {
        responseMap[gao] = responseRegister
    }

    fun commandInvoke(engineExecutor: EngineExecutor) {
        val trxId = engineExecutor.getDataHandler().getTrxId()
        unCommitGao.putIfAbsent(trxId, true)
        try {

            when (engineExecutor.getDataHandler().getCommandType()) {
                /*
                 * common 操作比较特殊，它直接会有些特殊交互，比如开启一个事务，关闭一个事务等。
                 */
                CommandTypeEnum.COMMON -> {
                    when (engineExecutor.getDataHandler().getApi()) {
                        CommonApiTypeEnum.START_TRX -> {
                            val bb = ByteBuffer.allocate(8)
                            bb.putLong(trxId)
                            bb.flip()
                            engineExecutor.getEngineResult().setKanashiEntry(ByteBufferKanashiEntry(CommandTypeEnum.COMMON, bb))
                            engineExecutor.shotSuccess()
                        }
                        CommonApiTypeEnum.COMMIT_TRX -> {
                            doCommit(trxId)
                            engineExecutor.shotSuccess()
                        }
                        CommonApiTypeEnum.ROLL_BACK -> {
                            doRollBack(trxId)
                            engineExecutor.shotSuccess()
                        }
                    }
                }

                CommandTypeEnum.STR -> {
                    when (engineExecutor.getDataHandler().getApi()) {
                        StrApiTypeEnum.SELECT -> {
                            engineDataQueryer.doQuery(engineExecutor, {
                                if (it != null && !it.isDelete()) {
                                    engineExecutor.getEngineResult().setKanashiEntry(it)
                                }
                                engineExecutor.shotSuccess()
                            }, false)
                        }

                        StrApiTypeEnum.DELETE -> {
                            doAcquire(engineExecutor) {
                                // 将值设置为被删除
                                engineExecutor.getDataHandler().markKanashiEntryAsDeleteBeforeOperate()
                                memoryMVCCStorageUnCommittedPart.commonOperate(engineExecutor.getDataHandler())
                                engineExecutor.shotSuccess()
                            }
                        }

                        StrApiTypeEnum.SET -> {
                            doAcquire(engineExecutor) {
                                memoryMVCCStorageUnCommittedPart.commonOperate(engineExecutor.getDataHandler())
                                engineExecutor.shotSuccess()
                            }
                        }

                        StrApiTypeEnum.SET_EXIST -> {
                            doAcquire(engineExecutor) {

                                engineExecutor.getDataHandler().getTrxId()
                                engineDataQueryer.doQuery(engineExecutor, {
                                    if (it == null || it.isDelete()) {
                                        engineExecutor.shotFailure()
                                    } else {
                                        memoryMVCCStorageUnCommittedPart.commonOperate(engineExecutor.getDataHandler())
                                        engineExecutor.shotSuccess()
                                    }
                                    // 这里必须使用当前读而不是快照读
                                }, true)
                            }
                        }

                        StrApiTypeEnum.SET_NOT_EXIST -> {
                            doAcquire(engineExecutor) {

                                engineExecutor.getDataHandler().getTrxId()
                                engineDataQueryer.doQuery(engineExecutor, {
                                    if (it != null && !it.isDelete()) {
                                        engineExecutor.shotFailure()
                                    } else {
                                        memoryMVCCStorageUnCommittedPart.commonOperate(engineExecutor.getDataHandler())
                                        engineExecutor.shotSuccess()
                                    }
                                    // 这里必须使用当前读而不是快照读
                                }, true)
                            }
                        }
                        StrApiTypeEnum.SET_IF -> {
                            doAcquire(engineExecutor) {

                                engineExecutor.getDataHandler().getTrxId()
                                engineDataQueryer.doQuery(engineExecutor, {
                                    if (it == null || it.getValueString() != engineExecutor.getDataHandler().extraParams[0]) {
                                        engineExecutor.shotFailure()
                                    } else {
                                        memoryMVCCStorageUnCommittedPart.commonOperate(engineExecutor.getDataHandler())
                                        engineExecutor.shotSuccess()
                                    }
                                    // 这里必须使用当前读而不是快照读
                                }, true)
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
        EventDriverPool.offer(engineExecutor)
    }

    /**
     * 进行事务控制与数据流转，通过无锁控制来进行阻塞与唤醒
     *
     * 如果拿到锁，则调用 whatEverDo
     *
     * 否则会“阻塞” 其实不是阻塞，是将任务进行保存，后续才会执行
     */
    private fun doAcquire(engineExecutor: EngineExecutor, whatEverDo: () -> Unit) {
        val dataHandler = engineExecutor.getDataHandler()
        val trxId = dataHandler.getTrxId()

        trxFreeQueuedSynchronizer.acquire(trxId, dataHandler.key, whatEverDo)
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
            keys?.let {
                if (unCommitGao[trxId] == true) {
                    memoryMVCCStorageUnCommittedPart.flushToCommittedPart(trxId, it)
                } else {
                    memoryMVCCStorageUnCommittedPart.discard(trxId, it)
                }
            }
            transactionManageService.releaseTrx(trxId)
            unCommitGao.remove(trxId)
        }
    }

    private fun doRollBack(trxId: Long) {
        unCommitGao[trxId] = false
        doCommit(trxId)
    }
}
