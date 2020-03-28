package ink.anur.engine.processor

import ink.anur.engine.trx.manager.TransactionManageService
import ink.anur.engine.trx.watermark.WaterMarkRegistry
import ink.anur.engine.trx.watermark.common.WaterMarker
import ink.anur.exception.WaterMarkCreationException
import ink.anur.inject.Nigate
import ink.anur.inject.NigateInject
import ink.anur.pojo.log.ByteBufferKanashiEntry
import ink.anur.pojo.log.KanashiCommand
import ink.anur.pojo.log.base.LogItem
import ink.anur.pojo.log.common.CommandTypeEnum
import ink.anur.pojo.log.common.TransactionTypeEnum
import java.rmi.UnexpectedException

/**
 * Created by Anur IjuoKaruKas on 2019/12/3
 *
 * 数据控制，需要用到很多数据，进入数据引擎是 LogItem（持有一个 kanashiCommand），持久化后又是 kanashiEntry
 *
 * 那么如何节省内存，实际上就是这个类所做的事情
 */
class DataHandler(private val logItem: LogItem) {

    @NigateInject
    private lateinit var waterMarkRegistry: WaterMarkRegistry

    @NigateInject
    private lateinit var transactionManageService: TransactionManageService

    /////////////////////////////////////////// init

    /**
     * 从 LogItem 中直接获取 key
     */
    val key = logItem.getKey()

    /**
     * 如果是短事务，操作完就直接提交
     *
     * 如果是长事务，则如果没有激活过事务，需要进行事务的激活(创建快照)
     */
    val shortTransaction = logItem.getKanashiCommand().transactionType == TransactionTypeEnum.SHORT

    /**
     * 短事务不需要快照，长事务则是有快照就用快照，没有就创建一个快照
     */
    var waterMarker: WaterMarker

    /**
     * 从 kanashiCommand 中的 byteBuffer 中获取额外的参数（第一个参数以外的参数）
     */
    val extraParams: MutableList<String>

    /**
     * kanashi Entry，里面存储着数据本身的类型（str，还是其他的，是否被删除，以及 value）
     */
    private var byteBufferKanashiEntry: ByteBufferKanashiEntry

    init {
        Nigate.injectOnly(this)
        val trxId = getTrxId()
        if (trxId == KanashiCommand.NON_TRX) {
            waterMarker = WaterMarker.NONE
        } else {
            waterMarker = waterMarkRegistry.findOut(trxId)
            // 如果没有水位快照，代表此事务从未激活过，需要去激活一下
            if (waterMarker == WaterMarker.NONE) {
                if (shortTransaction) {
                    transactionManageService.activateTrx(trxId, false)
                } else {
                    transactionManageService.activateTrx(trxId, true)

                    // 从事务控制器申请激活该事务
                    waterMarker = waterMarkRegistry.findOut(trxId)
                    if (waterMarker == WaterMarker.NONE) {
                        throw WaterMarkCreationException("事务 [$trxId] 创建事务快照失败")
                    }
                }
            }
        }

        // 取出额外参数
        extraParams = logItem.getKanashiCommand().extraParams
        byteBufferKanashiEntry = logItem.getKanashiCommand().kanashiEntry

        // 这后面的内存可以释放掉了
        logItem.getKanashiCommand().content.limit(KanashiCommand.TransactionSignOffset)
    }

    // ================================ ↓↓↓↓↓↓↓  直接访问 byteBuffer 拉取数据

    /**
     * 从 kanashiCommand 中的 byteBuffer 中获取 trxId
     */
    fun getTrxId(): Long = logItem.getKanashiCommand().trxId

    /**
     * 从 kanashiCommand 中的 byteBuffer 中获取 commandType
     */
    fun getCommandType() = logItem.getKanashiCommand().commandType

    /**
     * 从 kanashiCommand 中的 byteBuffer 中获取具体请求的 api
     */
    fun getApi() = logItem.getKanashiCommand().api

    /**
     * 在正式操作引擎之前将值设置为被删除
     */
    fun markKanashiEntryAsDeleteBeforeOperate() {
        byteBufferKanashiEntry = ByteBufferKanashiEntry.allocateEmptyKanashiEntry()
    }

    @Synchronized
    fun getKanashiEntry(): ByteBufferKanashiEntry {
        return byteBufferKanashiEntry
    }

    /**
     * 释放内存
     */
    fun destroy() {

    }
}