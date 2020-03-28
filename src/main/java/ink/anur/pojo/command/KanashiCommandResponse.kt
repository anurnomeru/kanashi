package ink.anur.pojo.command

import ink.anur.pojo.common.AbstractStruct
import ink.anur.pojo.enumerate.RequestTypeEnum
import ink.anur.pojo.log.ByteBufferKanashiEntry
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

    companion object {
        val SuccessOffset = OriginMessageOverhead
        val SuccessLength = 1
        val Capacity = SuccessOffset + SuccessLength
    }

    constructor(b: Boolean, entry: ByteBufferKanashiEntry?) {
        kanashiEntry = entry
        init(Capacity, RequestTypeEnum.COMMAND_RESPONSE) { it.put(translateToByte(b)) }
        success = b
        buffer!!.flip()
    }

    constructor(byteBuffer: ByteBuffer) {
        if (byteBuffer.limit() == OriginMessageOverhead) {
            kanashiEntry = null
        } else {
            byteBuffer.position(OriginMessageOverhead)
            kanashiEntry = ByteBufferKanashiEntry(byteBuffer.slice())
        }
        success = translateToBool(byteBuffer.get(SuccessOffset))
        byteBuffer.position(0)
        byteBuffer.limit(OriginMessageOverhead)
        buffer = byteBuffer.slice()
    }

    override fun writeIntoChannel(channel: Channel) {
        kanashiEntry?.byteBuffer?.position(0)
        channel.write(getByteBuffer())
        kanashiEntry?.let {
            channel.write(it.byteBuffer)
        }
        channel.flush()
    }

    override fun totalSize(): Int {
        return size() + (kanashiEntry?.byteBuffer?.limit() ?: 0)
    }
}