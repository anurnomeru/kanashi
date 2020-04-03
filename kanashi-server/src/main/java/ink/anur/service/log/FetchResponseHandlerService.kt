package ink.anur.service.log

import ink.anur.core.common.AbstractRequestMapping
import ink.anur.core.raft.ElectionMetaService
import ink.anur.debug.Debugger
import ink.anur.engine.log.LogService
import ink.anur.engine.prelog.ByteBufPreLogService
import ink.anur.inject.Event
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.inject.NigateListenerService
import ink.anur.pojo.enumerate.RequestTypeEnum
import ink.anur.pojo.log.common.GenerationAndOffset
import ink.anur.pojo.command.KanashiCommandBatchDto
import io.netty.channel.Channel
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2020/3/18
 *
 * 发送 fetch 以后，会返回 fetch 到的 log
 */
@NigateBean
class FetchResponseHandlerService : AbstractRequestMapping() {

    private val logger = Debugger(this.javaClass)

    @NigateInject
    private lateinit var electionMetaService: ElectionMetaService

    @NigateInject
    private lateinit var logService: LogService

    @NigateInject
    private lateinit var byteBufPreLogService: ByteBufPreLogService

    @NigateInject
    private lateinit var recoveryReportHandleService: RecoveryReportHandleService

    override fun typeSupport(): RequestTypeEnum {
        return RequestTypeEnum.FETCH_RESPONSE
    }

    override fun handleRequest(fromServer: String, msg: ByteBuffer, channel: Channel) {
        val commandContainer = KanashiCommandBatchDto(msg)

        if (!electionMetaService.isLeader()) {

            /**
             * 普通节点收到了日志，只需要简单将其追加到 byteBufPreLog 即可
             */
            if (commandContainer.fileLogItemSetSize == 0) return
            byteBufPreLogService.append(commandContainer.generation, commandContainer.read())
        } else {

            val fetchTo = recoveryReportHandleService.fetchTo!!

            /**
             * leader 收到了日志，说明集群正在恢复阶段，它做的比较特殊，
             * 它直接将日志追加到了 logService 进行刷盘
             */
            val read = commandContainer.read()
            val iterator = read.iterator()

            val gen = commandContainer.generation
            val fetchToGen = fetchTo.generation

            var start: Long? = null
            var end: Long? = null

            iterator.forEach {

                if (start == null) start = it.offset
                end = it.offset

                // 集群恢复
                logService.appendWhileRecovery(gen, it.offset, it.logItem)

                if (gen == fetchToGen) {
                    val offset = it.offset
                    val fetchToOffset = fetchTo.offset
                    if (offset == fetchToOffset) {// 如果已经同步完毕，则通知集群同步完成
                        recoveryReportHandleService.shuttingWhileRecoveryComplete(fetchTo)
                        recoveryReportHandleService.fetchTo = null
                    }
                }
            }

            byteBufPreLogService.cover(GenerationAndOffset(gen, end!!))
            logger.debug("集群日志恢复：追加 gen = {$gen} offset-start {$start} end {$end} 的日志段完毕")
        }
    }
}