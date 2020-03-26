package ink.anur.service.command

import ink.anur.core.common.AbstractRequestMapping
import ink.anur.core.raft.ElectionMetaService
import ink.anur.engine.log.LogService
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.pojo.enumerate.RequestTypeEnum
import ink.anur.pojo.command.KanashiCommandDto
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
    private lateinit var

    override fun typeSupport(): RequestTypeEnum {
        return RequestTypeEnum.COMMAND
    }

    override fun handleRequest(fromServer: String, msg: ByteBuffer, channel: Channel) {
        val logItem = KanashiCommandDto(msg).logItem
        val kanashiCommand = logItem.getKanashiCommand()
        if (!kanashiCommand.isQueryCommand && !electionMetaService.isLeader()) {
            // 发送谁才是leader
        } else if (kanashiCommand.isQueryCommand) {

        } else {

        }
    }
}