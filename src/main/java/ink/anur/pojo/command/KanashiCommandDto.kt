package ink.anur.pojo.command

import ink.anur.pojo.common.AbstractTimedStruct
import ink.anur.pojo.enumerate.RequestTypeEnum
import ink.anur.pojo.log.KanashiCommand
import ink.anur.pojo.log.base.LogItem
import ink.anur.pojo.log.common.CommandTypeEnum
import ink.anur.pojo.log.common.StrApiTypeEnum
import ink.anur.pojo.log.common.TransactionTypeEnum
import io.netty.buffer.Unpooled
import io.netty.channel.Channel
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2020/3/26
 *
 * 单条的 command
 */
class KanashiCommandDto : AbstractTimedStruct {

    val logItem: LogItem

    constructor(l: LogItem) {
        logItem = l
        super.init(OriginMessageOverhead, RequestTypeEnum.COMMAND) {}
    }

    constructor(byteBuffer: ByteBuffer) {
        val limit = byteBuffer.limit()
        byteBuffer.limit(OriginMessageOverhead)
        buffer = byteBuffer.slice()
        byteBuffer.position(OriginMessageOverhead)
        byteBuffer.limit(limit)
        logItem = LogItem(byteBuffer.slice())
    }

    override fun writeIntoChannel(channel: Channel) {
        channel.write(Unpooled.wrappedBuffer(buffer!!))
        channel.write(logItem.getByteBuffer())
    }

    override fun totalSize(): Int {
        return size() + logItem.getByteBuffer()!!.limit()
    }
}