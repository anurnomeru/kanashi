package ink.anur.service.command

import ink.anur.common.struct.KanashiNode
import ink.anur.config.InetSocketAddressConfiguration
import ink.anur.core.common.AbstractRequestMapping
import ink.anur.core.raft.ElectionMetaService
import ink.anur.core.raft.RaftCenterController
import ink.anur.core.request.RequestProcessCentreService
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
import ink.anur.pojo.command.KanashiCommandResponse
import ink.anur.pojo.enumerate.RequestTypeEnum
import ink.anur.pojo.log.KanashiCommand
import ink.anur.pojo.log.base.LogItem
import ink.anur.pojo.log.common.CommandTypeEnum
import ink.anur.pojo.log.common.CommonApiTypeEnum
import ink.anur.pojo.log.common.GenerationAndOffset
import ink.anur.pojo.log.common.TransactionTypeEnum
import io.netty.channel.Channel
import java.nio.ByteBuffer
import javax.smartcardio.CommandAPDU

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

    @NigateInject
    private lateinit var requestProcessCentreService: RequestProcessCentreService

    @NigateInject
    private lateinit var inetSocketAddressConfiguration: InetSocketAddressConfiguration

    override fun typeSupport(): RequestTypeEnum {
        return RequestTypeEnum.COMMAND
    }

    companion object{
        @JvmStatic
        fun main(args: Array<String>) {
            val logItem00 = LogItem("Anur", KanashiCommand.generator(1, TransactionTypeEnum.LONG, CommandTypeEnum.COMMON, CommonApiTypeEnum.GET_CLUSTER, ""))
            val logItem = LogItem(LogItem("Anur", KanashiCommand.generator(1, TransactionTypeEnum.LONG, CommandTypeEnum.COMMON, CommonApiTypeEnum.GET_CLUSTER, "")).getByteBuffer()!!)
            logItem.getKanashiCommand().resetTransactionId(2)
            logItem.reComputeCheckSum()

            println()

            val byteBuffer = logItem00.getByteBuffer()!!
            val limit = byteBuffer.limit()

            while (byteBuffer.position()<limit){
                println(byteBuffer.get())
                println(logItem.getByteBuffer()!!.get())
            }
        }
    }

    // TODO 需要整理 这里太乱了
    override fun handleRequest(fromServer: String, msg: ByteBuffer, channel: Channel) {
        val logItem = LogItem(msg)
        val kanashiCommand = logItem.getKanashiCommand()

        if (!electionMetaService.clusterValid) {
            // todo 还没选举好 暂时不报错
            return
        } else {

            /**
             * 首先两种请求最特殊，就是查询与获取
             */
            if (kanashiCommand.isQueryCommand) {
                // 如果是查询请求，直接请求存储引擎
                val engineExecutor = EngineExecutor(DataHandler(EngineProcessEntry(logItem, GenerationAndOffset.INVALID)), ResponseRegister(logItem.getTimeMillis(), fromServer))
                storeEngineTransmitService.commandInvoke(engineExecutor)
            } else if (!electionMetaService.isLeader() || kanashiCommand.commandType == CommandTypeEnum.COMMON && kanashiCommand.api == CommonApiTypeEnum.GET_CLUSTER) {
                // 不是leader 或者 请求获取集群，返回集群信息

                val leader = electionMetaService.getLeader()
                val leaderNode = inetSocketAddressConfiguration.getNode(leader)
                val clusters = electionMetaService.clusters?.let { ArrayList(it) }
                if (leaderNode != KanashiNode.NOT_EXIST && clusters != null) {
                    // 将 leader 节点放在首位
                    clusters.removeIf { it == leaderNode }
                    clusters.add(0, leaderNode)
                    requestProcessCentreService.send(fromServer,
                        KanashiCommandResponse.genCluster(logItem.getTimeMillis(), clusters))
                }
                return

            } else {

                /**
                 * 除了上述两种请求，其他的请求必须经过 leader
                 */
                when (kanashiCommand.transactionType) {
                    TransactionTypeEnum.SHORT -> {
                        kanashiCommand.resetTransactionId(transactionAllocator.allocate())
                        logItem.reComputeCheckSum()
                    }
                    TransactionTypeEnum.LONG -> {
                        if (kanashiCommand.trxId == KanashiCommand.NON_TRX) {
                            if (kanashiCommand.commandType == CommandTypeEnum.COMMON && kanashiCommand.api == CommonApiTypeEnum.START_TRX) {
                                kanashiCommand.resetTransactionId(transactionAllocator.allocate())
                                logItem.reComputeCheckSum()
                            } else {
                                // 不允许长事务不带事务id
                                requestProcessCentreService.send(fromServer, KanashiCommandResponse.genError(logItem.getTimeMillis(), "不允许长事务无事务id"))
                                return
                            }
                        }
                    }
                }

                val gao = try {
                    raftCenterController.genGenerationAndOffset()
                } catch (e: NotLeaderException) {
                    handleRequest(fromServer, msg, channel)
                    return
                }

                storeEngineTransmitService.waitForResponse(gao, ResponseRegister(logItem.getTimeMillis(), fromServer))
                logService.appendForLeader(gao, logItem)
            }
        }
    }
}