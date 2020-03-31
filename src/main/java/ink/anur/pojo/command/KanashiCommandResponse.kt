package ink.anur.pojo.command

import ink.anur.pojo.common.AbstractStruct
import ink.anur.pojo.enumerate.RequestTypeEnum
import ink.anur.pojo.log.ByteBufferKanashiEntry
import io.netty.buffer.Unpooled
import io.netty.channel.Channel
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2020/3/28
 *
 * 成功的回复
 */
class KanashiCommandResponse : AbstractStruct {

    val kanashiEntry: ByteBufferKanashiEntry?

    val success: Boolean

    val msgTime: Long

    companion object {
        val SuccessOffset = OriginMessageOverhead
        val SuccessLength = 1
        val msgTimeOffset = SuccessOffset + SuccessLength
        val msgTimeLength = 8
        val Capacity = msgTimeOffset + msgTimeLength
    }

    constructor(msgTime: Long, b: Boolean, entry: ByteBufferKanashiEntry?) {
        kanashiEntry = entry
        init(Capacity, RequestTypeEnum.COMMAND_RESPONSE) {
            it.put(translateToByte(b))
            it.putLong(msgTime)
        }
        this.msgTime = msgTime
        this.success = b
    }

    constructor(byteBuffer: ByteBuffer) {
        if (byteBuffer.limit() == Capacity) {
            kanashiEntry = null
        } else {
            byteBuffer.position(Capacity)
            kanashiEntry = ByteBufferKanashiEntry(byteBuffer.slice())
        }
        this.success = translateToBool(byteBuffer.get(SuccessOffset))
        this.msgTime = byteBuffer.getLong(msgTimeOffset)
        byteBuffer.position(0)
        byteBuffer.limit(Capacity)
        buffer = byteBuffer.slice()
    }

    override fun writeIntoChannel(channel: Channel) {
        kanashiEntry?.byteBuffer?.position(0)
        channel.write(Unpooled.wrappedBuffer(getByteBuffer()))
        kanashiEntry?.let {
            channel.write(Unpooled.wrappedBuffer(it.byteBuffer))
        }
    }

    override fun totalSize(): Int {
        return size() + (kanashiEntry?.byteBuffer?.limit() ?: 0)
    }
}