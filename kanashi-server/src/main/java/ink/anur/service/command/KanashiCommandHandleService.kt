package ink.anur.service.command

import ink.anur.core.common.AbstractRequestMapping
import ink.anur.core.raft.ElectionMetaService
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.pojo.enumerate.RequestTypeEnum
import ink.anur.pojo.server.KanashiCommandContainer
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

    override fun typeSupport(): RequestTypeEnum {
        return RequestTypeEnum.COMMAND;
    }

    override fun handleRequest(fromServer: String, msg: ByteBuffer, channel: Channel) {
        val commandContainer = KanashiCommandContainer(msg)

        val byteBufferLogItemSet = commandContainer.read()
        val iterator = byteBufferLogItemSet.iterator()

        while (iterator.hasNext()){
            val logItemAndOffset = iterator.next()
            val kanashiCommand = logItemAndOffset.logItem.getKanashiCommand()
        }

    }
}