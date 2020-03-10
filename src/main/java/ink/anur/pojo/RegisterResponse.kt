package ink.anur.pojo

import ink.anur.pojo.common.AbstractTimedStruct
import ink.anur.pojo.enumerate.RequestTypeEnum
import io.netty.buffer.Unpooled
import io.netty.channel.Channel
import java.nio.ByteBuffer

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
        init(byteBuffer, RequestTypeEnum.REGISTER_RESPONSE)
        byteBuffer.flip()
    }

    constructor(byteBuffer: ByteBuffer) {
        buffer = byteBuffer
    }

    override fun writeIntoChannel(channel: Channel) {
        val wrappedBuffer = Unpooled.wrappedBuffer(buffer)
        channel.write(wrappedBuffer)
        wrappedBuffer.release()
    }

    override fun totalSize(): Int {
        return size()
    }
}