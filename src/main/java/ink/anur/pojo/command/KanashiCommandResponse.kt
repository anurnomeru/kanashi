package ink.anur.pojo.command

import ink.anur.common.struct.KanashiNode
import ink.anur.pojo.common.AbstractStruct
import ink.anur.pojo.enumerate.RequestTypeEnum
import ink.anur.pojo.log.ByteBufferKanashiEntry
import ink.anur.pojo.log.common.CommandTypeEnum
import io.netty.buffer.Unpooled
import io.netty.channel.Channel
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2020/3/28
 *
 * 对于command请求的回复
 */
class KanashiCommandResponse : AbstractStruct {

    val kanashiEntry: ByteBufferKanashiEntry?

    /**
     * 判断请求是否成功，如果成功，就没有异常，也不需要转发
     */
    val success: Boolean

    /**
     * 代表这个回复中抛出了异常
     */
    val error: Boolean

    /**
     * 代表此请求需要转发至 Leader
     */
    val redirect: Boolean

    val msgTime: Long

    companion object {
        val SuccessOffset = OriginMessageOverhead
        val SuccessLength = 1
        val ErrorOffset = SuccessOffset + SuccessLength
        val ErrorLength = 1
        val RedirectOffset = ErrorOffset + ErrorLength
        val RedirectLength = 1
        val msgTimeOffset = RedirectOffset + RedirectLength
        val msgTimeLength = 8
        val Capacity = msgTimeOffset + msgTimeLength

        fun genError(msgTime: Long, msg: String): KanashiCommandResponse {
            val toByteArray = msg.toByteArray()
            val allocate = ByteBuffer.allocate(toByteArray.size)
            allocate.put(toByteArray)
            allocate.flip()

            return KanashiCommandResponse(msgTime, false, ByteBufferKanashiEntry(CommandTypeEnum.NONE, allocate), error = true, redirect = false)
        }

        /**
         * 发送集群当前状态，第一个位置的是 leader
         */
        fun genCluster(msgTime: Long, cluster: List<KanashiNode>): KanashiCommandResponse {
            val list = arrayListOf<ByteArray>()
            for (kanashiNode in cluster) {
                list.add("${kanashiNode.serverName}:${kanashiNode.host}:${kanashiNode.port}".toByteArray())
            }
            val capacity = list.size * 4 + list.map { it.size }.reduce { i1, i2 -> i1 + i2 }

            val allocate = ByteBuffer.allocate(capacity)
            for (bytes in list) {
                allocate.putInt(bytes.size)
                allocate.put(bytes)
            }
            allocate.flip()
            return KanashiCommandResponse(msgTime, false, ByteBufferKanashiEntry(CommandTypeEnum.NONE, allocate), error = true, redirect = true)
        }
    }

    constructor(msgTime: Long, b: Boolean, entry: ByteBufferKanashiEntry?, error: Boolean = false, redirect: Boolean = false) {
        kanashiEntry = entry
        init(Capacity, RequestTypeEnum.COMMAND_RESPONSE) {
            it.put(translateToByte(b))
            it.put(translateToByte(error))
            it.put(translateToByte(redirect))
            it.putLong(msgTime)
        }
        this.msgTime = msgTime
        this.success = b
        this.error = false
        this.redirect = false
    }

    constructor(byteBuffer: ByteBuffer) {
        if (byteBuffer.limit() == Capacity) {
            kanashiEntry = null
        } else {
            byteBuffer.position(Capacity)
            kanashiEntry = ByteBufferKanashiEntry(byteBuffer.slice())
        }
        this.success = translateToBool(byteBuffer.get(SuccessOffset))
        this.error = translateToBool(byteBuffer.get(ErrorOffset))
        this.redirect = translateToBool(byteBuffer.get(RedirectOffset))
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