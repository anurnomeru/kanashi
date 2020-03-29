package ink.anur

import ink.anur.config.BootstrapConfiguration
import ink.anur.engine.log.LogService
import ink.anur.engine.trx.manager.TransactionAllocator
import ink.anur.inject.Nigate
import ink.anur.pojo.enumerate.RequestTypeEnum
import ink.anur.pojo.log.KanashiCommand
import ink.anur.pojo.log.base.LogItem
import ink.anur.pojo.log.common.CommandTypeEnum
import ink.anur.pojo.log.common.StrApiTypeEnum
import ink.anur.pojo.log.common.TransactionTypeEnum

/**
 * Created by Anur IjuoKaruKas on 2020/2/22
 */
object Bootstrap {

    @Volatile
    private var RUNNING = true

    @JvmStatic
    fun main(args: Array<String>) {
        // 保存jvm参数
        BootstrapConfiguration.init(args)
        // 初始化 bean管理
        Nigate

        var i = 0

        try {

//            Thread.sleep(5000)

//            for (i in 0..99999999) {
//            for (i in 0..1000000) {
//
//                val transactionAllocator = Nigate.getBeanByClass(TransactionAllocator::class.java)
//
////                val logItem = LogItem(RequestTypeEnum.LOG_ITEM, "AnurKey",
////                    KanashiCommand.generator(
////                        transactionAllocator.allocate(), TransactionTypeEnum.SHORT, CommandTypeEnum.STR, StrApiTypeEnum.SET, "kanashiValue-中文-"))
//
//                val logItem = LogItem(RequestTypeEnum.COMMAND, "AnurKey",
//                    KanashiCommand.generator(
//                        Long.MAX_VALUE, TransactionTypeEnum.SHORT, CommandTypeEnum.STR, StrApiTypeEnum.SELECT))
//
//                val logService = Nigate.getBeanByClass(LogService::class.java)
//                logService.appendForLeader(logItem)
//            }

//            println("append complete")
        } catch (e: Exception) {
            throw e
        }


        while (RUNNING) {
            i++
            Thread.sleep(1000)
        }
    }
}