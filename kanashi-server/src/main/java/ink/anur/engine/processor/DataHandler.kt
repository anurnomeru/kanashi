package ink.anur.engine.processor

import ink.anur.debug.Debugger
import ink.anur.engine.trx.manager.TrxManageService
import ink.anur.engine.trx.watermark.WaterMarkRegistry
import ink.anur.engine.trx.watermark.common.WaterMarker
import ink.anur.exception.WaterMarkCreationException
import ink.anur.inject.Nigate
import ink.anur.inject.NigateInject
import ink.anur.inject.NigatePostConstruct
import ink.anur.pojo.log.ByteBufferKanashiEntry
import ink.anur.pojo.log.KanashiCommand
import ink.anur.pojo.log.base.LogItem
import ink.anur.pojo.log.common.CommandTypeEnum
import ink.anur.pojo.log.common.TransactionTypeEnum
import java.rmi.UnexpectedException

/**
 * Created by Anur IjuoKaruKas on 2019/12/3
 *
 * 数据控制，需要用到很多数据，进入数据引擎是 Operation（持有一个 HanabiCommand），持久化后又是 HanabiEntry
 *
 * 那么如何节省内存，实际上就是这个类所做的事情
 */
class DataHandler(val operation: LogItem) {

    companion object {
        val logger = Debugger(DataHandler.javaClass)

        @NigateInject
        private lateinit var waterMarkRegistry: WaterMarkRegistry

        @NigateInject
        private lateinit var trxManageService: TrxManageService

        @NigatePostConstruct
        private fun init() {
            Nigate.injectOnly(this)
        }
    }

    /////////////////////////////////////////// init

    /**
     * 从 Operation 中直接获取 key
     */
    val key = operation.getKey()

    /**
     * 如果是短事务，操作完就直接提交
     *
     * 如果是长事务，则如果没有激活过事务，需要进行事务的激活(创建快照)
     */
    val shortTransaction = TransactionTypeEnum.map(operation.getKanashiCommand().getTransactionType()) == TransactionTypeEnum.SHORT

    /**
     * 短事务不需要快照，长事务则是有快照就用快照，没有就创建一个快照
     */
    var waterMarker: WaterMarker

    /**
     * 从 hanabiCommand 中的 byteBuffer 中获取额外的参数（第一个参数以外的参数）
     */
    val extraParams: MutableList<String>

    /**
     * hanabi Entry，里面存储着数据本身的类型（str，还是其他的，是否被删除，以及 value）
     */
    private val byteBufferHanabiEntry: ByteBufferKanashiEntry

    init {
        val trxId = getTrxId()
        waterMarker = waterMarkRegistry.findOut(trxId)
        // 如果没有水位快照，代表此事务从未激活过，需要去激活一下
        if (waterMarker == WaterMarker.NONE) {
            if (shortTransaction) {
                trxManageService.activateTrx(trxId, false)
            } else {
                trxManageService.activateTrx(trxId, true)

                // 从事务控制器申请激活该事务
                waterMarker = waterMarkRegistry.findOut(trxId)
                if (waterMarker == WaterMarker.NONE) {
                    throw WaterMarkCreationException("事务 [$trxId] 创建事务快照失败")
                }
            }
        }

        // 取出额外参数
        extraParams = operation.getKanashiCommand().getExtraValues()
        byteBufferHanabiEntry = operation.getKanashiCommand().getKanashiEntry()

        // 这后面的内存可以释放掉了
        operation.getKanashiCommand().content.limit(KanashiCommand.TransactionSignOffset)
    }

    // ================================ ↓↓↓↓↓↓↓  直接访问 byteBuffer 拉取数据

    /**
     * 从 hanabiCommand 中的 byteBuffer 中获取 trxId
     */
    fun getTrxId() = operation.getKanashiCommand().getTrxId()

    /**
     * 从 hanabiCommand 中的 byteBuffer 中获取 commandType
     */
    fun getCommandType() = CommandTypeEnum.map(operation.getKanashiCommand().getCommandType())

    /**
     * 从 hanabiCommand 中的 byteBuffer 中获取具体请求的 api
     */
    fun getApi() = operation.getKanashiCommand().getApi()

    /**
     * 除了select操作，其余操作必须指定这个
     */
    fun setOperateType(operateType: ByteBufferKanashiEntry.Companion.OperateType) {
        byteBufferHanabiEntry.setOperateType(operateType)
    }

    @Synchronized
    fun genHanabiEntry(): ByteBufferKanashiEntry {
        if (!byteBufferHanabiEntry.operateTypeSet) {
            throw UnexpectedException("operateType 在进行非查询操作时必须指定！ 估计是代码哪里有 bug 导致没有指定！")
        }
        return byteBufferHanabiEntry
    }

    /**
     * 释放内存
     */
    fun destroy() {

    }
}