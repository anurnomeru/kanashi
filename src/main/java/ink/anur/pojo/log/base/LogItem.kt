package ink.anur.pojo.log.base

import ink.anur.pojo.common.AbstractStruct
import ink.anur.pojo.enumerate.RequestTypeEnum
import ink.anur.pojo.log.KanashiCommand
import ink.anur.pojo.log.common.CommandTypeEnum
import ink.anur.pojo.log.common.TransactionTypeEnum
import io.netty.buffer.Unpooled
import io.netty.channel.Channel
import java.nio.ByteBuffer
import java.nio.charset.Charset

/**
 * Created by Anur IjuoKaruKas on 2/25/2019
 * <p>
 * LogItem 是对应最基本的操作，表示这个操作将被写入日志
 * <p>
 * 一个 logItem 由以下部分组成：
 * <p>
 * 　4　   +   4    +    4      + key +    4        +  v
 * CRC32  +  type  + keyLength + key + valueLength +  v
 */
open class LogItem : AbstractStruct {

    companion object {
        val KeySizeOffset = OriginMessageOverhead

        val KeySizeLength = 4

        val KeyOffset = KeySizeOffset + KeySizeLength

        /**
         * 一定要有key
         */
        val MinKeyLength = 1

        val ValueSizeLength = 4

        /**
         * 除去消息头，最小的 LogItem 长度为这个，小于这个不可能构成一条消息，最起码要满足
         *
         *
         * CRC32 +  type  + (KeySize = 1) + key + (ValueSize = 1)
         */
        val MinMessageOverhead = KeyOffset + ValueSizeLength + MinKeyLength

        /**
         * 最基础的 LogItem 大小
         */
        val BaseMessageOverhead = KeyOffset + ValueSizeLength

        // =================================================================

    }

    private var key: String? = null

    private var kanashiCommand: KanashiCommand? = null

    constructor(key: String, value: KanashiCommand) {

        this.key = key
        this.kanashiCommand = value

        val kBytes = key.toByteArray(Charset.defaultCharset())
        val kSize = kBytes.size

        val vSize = value.contentLimit
        init(BaseMessageOverhead + kSize + vSize, RequestTypeEnum.COMMAND) {
            it.putInt(kSize)
            it.put(kBytes)
            it.putInt(vSize)
            it.put(value.content)
        }
        reComputeCheckSum()
    }

    constructor(buffer: ByteBuffer) {
        buffer.mark()
        this.buffer = buffer

        buffer.position(KeySizeOffset)

        val kSize = buffer.int
        val kByte = ByteArray(kSize)
        buffer.get(kByte)
        this.key = String(kByte)

        val vSizeIgnore = buffer.int
        this.kanashiCommand = KanashiCommand(buffer.slice())

        ensureValid()
        buffer.reset()
    }

    fun getKey(): String {
        return key!!
    }

    fun getKanashiCommand(): KanashiCommand {
        return kanashiCommand!!
    }

    override fun toString(): String {
        return "LogItem{" +
            "requestType='" + getRequestType() + '\''.toString() +
            ", key='" + key + '\''.toString() +
            ", kanashiEntry='" + kanashiCommand + '\''.toString() +
            '}'.toString()
    }

    override fun writeIntoChannel(channel: Channel) {
        buffer!!.position(0)
        channel.write(Unpooled.wrappedBuffer(buffer!!))
    }

    override fun totalSize(): Int {
        return size()
    }
}