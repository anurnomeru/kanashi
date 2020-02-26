package ink.anur.common.struct

import ink.anur.common.struct.common.AbstractTimedStruct
import ink.anur.common.struct.enumerate.OperationTypeEnum
import io.netty.buffer.Unpooled
import io.netty.channel.Channel
import io.netty.util.internal.StringUtil
import java.nio.ByteBuffer
import java.nio.charset.Charset

/**
 * Created by Anur IjuoKaruKas on 2020/2/24
 *
 * 用于协调节点向 Leader 注册自己
 */
open class Register : AbstractTimedStruct {

    companion object {
        val SizeOffset = OriginMessageOverhead
        val SizeLength = 4
        val ContentOffset = SizeOffset + SizeLength
    }

    private var serverName: String

    constructor(serverName: String) {
        this.serverName = serverName

        val bytes = serverName.toByteArray(Charset.defaultCharset())
        val size = bytes.size
        val byteBuffer = ByteBuffer.allocate(ContentOffset + size)
        init(byteBuffer, OperationTypeEnum.REGISTER)

        byteBuffer.putInt(size)
        byteBuffer.put(bytes)
        byteBuffer.flip()
    }

    constructor(byteBuffer: ByteBuffer) {
        buffer = byteBuffer
        val size = byteBuffer.getInt(SizeOffset)

        byteBuffer.position(ContentOffset)
        val bytes = ByteArray(size)
        byteBuffer.get(bytes)
        this.serverName = String(bytes)

        byteBuffer.rewind()
    }

    fun getServerName(): String {
        return serverName
    }

    override fun writeIntoChannel(channel: Channel) {
        channel.write(Unpooled.wrappedBuffer(buffer))
    }

    override fun totalSize(): Int {
        return size()
    }
}