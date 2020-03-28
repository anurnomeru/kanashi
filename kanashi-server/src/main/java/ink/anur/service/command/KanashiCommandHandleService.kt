package ink.anur.service.command

import ink.anur.core.common.AbstractRequestMapping
import ink.anur.core.raft.ElectionMetaService
import ink.anur.engine.StoreEngineFacadeService
import ink.anur.engine.log.LogService
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.log.common.EngineProcessEntry
import ink.anur.pojo.enumerate.RequestTypeEnum
import ink.anur.pojo.log.base.LogItem
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
    private lateinit var logService: LogService

    @NigateInject
    private lateinit var storeEngineFacadeService: StoreEngineFacadeService

    override fun typeSupport(): RequestTypeEnum {
        return RequestTypeEnum.COMMAND
    }

    override fun handleRequest(fromServer: String, msg: ByteBuffer, channel: Channel) {
        val logItem = LogItem(msg)
        val kanashiCommand = logItem.getKanashiCommand()
        if (!kanashiCommand.isQueryCommand && !electionMetaService.isLeader()) {
            // 发送谁才是leader
        } else if (kanashiCommand.isQueryCommand) {

        } else {
            // 如果是普通的请求，则直接存成日志，等待集群commit，返回成功
            val gao = logService.appendForLeader(logItem)

            // 添加到引擎，等待commit，commit后，会自动给请求方一个答复
            storeEngineFacadeService.appendToEngine(EngineProcessEntry(logItem, gao, fromServer))
        }
    }
}