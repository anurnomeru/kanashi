package ink.anur.service.command

import ink.anur.core.common.AbstractRequestMapping
import ink.anur.core.raft.ElectionMetaService
import ink.anur.core.raft.RaftCenterController
import ink.anur.engine.StoreEngineFacadeService
import ink.anur.engine.log.LogService
import ink.anur.engine.trx.manager.TransactionAllocator
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.log.common.EngineProcessEntry
import ink.anur.pojo.enumerate.RequestTypeEnum
import ink.anur.pojo.log.KanashiCommand
import ink.anur.pojo.log.base.LogItem
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
    private lateinit var storeEngineFacadeService: StoreEngineFacadeService

    override fun typeSupport(): RequestTypeEnum {
        return RequestTypeEnum.COMMAND
    }

    override fun handleRequest(fromServer: String, msg: ByteBuffer, channel: Channel) {
        val logItem = LogItem(msg)
        val kanashiCommand = logItem.getKanashiCommand()
        if (kanashiCommand.isQueryCommand) {
            storeEngineFacadeService.appendToEngine(EngineProcessEntry(logItem, null, fromServer, logItem.getTimeMillis()))
        } else {
            if (!electionMetaService.isLeader()) {
                // 不是 leader 的话，向对方发送谁才是 leader
            } else {

                if (kanashiCommand.transactionType == TransactionTypeEnum.SHORT) {
                    kanashiCommand.trxId = transactionAllocator.allocate()
                } else {
                    if (kanashiCommand.trxId == KanashiCommand.NON_TRX) {
                        // 不允许长事务不带事务id
                    }
                }

                // 如果是普通的请求，则直接存成日志，等待集群commit，返回成功
                val gao = logService.appendForLeader(logItem)

                // 添加到引擎，等待commit，commit后，会自动给请求方一个答复
                storeEngineFacadeService.appendToEngine(EngineProcessEntry(logItem, gao, fromServer, logItem.getTimeMillis()))
            }
        }
    }
}