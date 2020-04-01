package ink.anur.service.command

import ink.anur.core.common.AbstractRequestMapping
import ink.anur.core.raft.ElectionMetaService
import ink.anur.core.raft.RaftCenterController
import ink.anur.engine.StoreEngineFacadeService
import ink.anur.engine.StoreEngineTransmitService
import ink.anur.engine.log.LogService
import ink.anur.engine.processor.DataHandler
import ink.anur.engine.processor.EngineExecutor
import ink.anur.engine.processor.ResponseRegister
import ink.anur.engine.trx.manager.TransactionAllocator
import ink.anur.exception.NotLeaderException
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.log.common.EngineProcessEntry
import ink.anur.pojo.enumerate.RequestTypeEnum
import ink.anur.pojo.log.KanashiCommand
import ink.anur.pojo.log.base.LogItem
import ink.anur.pojo.log.common.CommandTypeEnum
import ink.anur.pojo.log.common.CommonApiTypeEnum
import ink.anur.pojo.log.common.GenerationAndOffset
import ink.anur.pojo.log.common.StrApiTypeEnum
import ink.anur.pojo.log.common.TransactionTypeEnum
import io.netty.channel.Channel
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2020/3/26
 *
 * 接收指令的处理器
 */
@NigateBean
class KanashiCommandHandleService : AbstractRequestMapping() {

    @NigateInject
    private lateinit var electionMetaService: ElectionMetaService

    @NigateInject
    private lateinit var transactionAllocator: TransactionAllocator

    @NigateInject
    private lateinit var logService: LogService

    @NigateInject
    private lateinit var raftCenterController: RaftCenterController

    @NigateInject
    private lateinit var storeEngineTransmitService: StoreEngineTransmitService

    override fun typeSupport(): RequestTypeEnum {
        return RequestTypeEnum.COMMAND
    }

    override fun handleRequest(fromServer: String, msg: ByteBuffer, channel: Channel) {
        val logItem = LogItem(msg)
        val kanashiCommand = logItem.getKanashiCommand()

        // 如果是查询请求，直接请求存储引擎
        if (kanashiCommand.isQueryCommand) {
            val engineExecutor = EngineExecutor(DataHandler(EngineProcessEntry(logItem, GenerationAndOffset.INVALID)), ResponseRegister(logItem.getTimeMillis(), fromServer))
            storeEngineTransmitService.commandInvoke(engineExecutor)
        } else {
            if (!electionMetaService.isLeader()) {
                // 不是 leader 的话，向对方发送谁才是 leader
            } else {

                // 如果是其他请求，则在此申请事务 id
                if (kanashiCommand.transactionType == TransactionTypeEnum.SHORT) {
                    kanashiCommand.trxId = transactionAllocator.allocate()
                } else {
                    if (kanashiCommand.trxId == KanashiCommand.NON_TRX) {
                        if (kanashiCommand.commandType==CommandTypeEnum.COMMON && kanashiCommand.api==CommonApiTypeEnum.START_TRX){
                            kanashiCommand.trxId = transactionAllocator.allocate()
                        }else{
                            // 不允许长事务不带事务id
                            // todo 抛出异常告知失败
                        }
                    }
                }

                // 如果是普通的请求，则直接存成日志，等待集群commit，返回成功
                val gao = try {
                    raftCenterController.genGenerationAndOffset()
                } catch (e: NotLeaderException) {
                    handleRequest(fromServer, msg, channel)
                    return
                }

                logService.appendForLeader(gao, logItem)
                storeEngineTransmitService.waitForResponse(gao, ResponseRegister(logItem.getTimeMillis(), fromServer))
            }
        }
    }
}