package ink.anur.struct

import ink.anur.struct.common.AbstractTimedStruct
import ink.anur.struct.enumerate.OperationTypeEnum
import io.netty.buffer.Unpooled
import io.netty.channel.Channel
import io.netty.util.internal.StringUtil
import java.nio.ByteBuffer
import java.nio.charset.Charset

/**
 * Created by Anur IjuoKaruKas on 2020/2/24
 *
 * 注册的 response 里面是空的 只是回复一下
 */
open class RegisterResponse : AbstractTimedStruct {

    companion object {
        val SizeOffset = OriginMessageOverhead
    }

    constructor() {
        val byteBuffer = ByteBuffer.allocate(SizeOffset)
        init(byteBuffer, OperationTypeEnum.REGISTER_RESPONSE)
        byteBuffer.flip()
    }

    constructor(byteBuffer: ByteBuffer) {
        buffer = byteBuffer
    }

    override fun writeIntoChannel(channel: Channel) {
        channel.write(Unpooled.wrappedBuffer(buffer))
    }

    override fun totalSize(): Int {
        return size()
    }
}