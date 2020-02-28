package ink.anur.struct

import ink.anur.struct.common.AbstractTimedStruct
import ink.anur.struct.enumerate.OperationTypeEnum
import io.netty.buffer.Unpooled
import io.netty.channel.Channel
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2020/2/26
 *
 * 投票 ~
 */
class Canvass : AbstractTimedStruct {

    companion object {
        val GenerationOffset = OriginMessageOverhead
        val GenerationLength = 8
        val Capacity = GenerationOffset + GenerationLength
    }

    /**
     * 该选票的世代信息
     */
    var generation: Long = 0

    constructor( generation: Long) {
        this.generation = generation

        val byteBuffer = ByteBuffer.allocate(Capacity)
        init(byteBuffer, OperationTypeEnum.VOTING)

        byteBuffer.putLong(generation)
        byteBuffer.flip()
    }

    constructor(byteBuffer: ByteBuffer) {
        byteBuffer.mark()
        byteBuffer.position(GenerationOffset)

        this.generation = byteBuffer.getLong()
        buffer!!.reset()
    }

    override fun writeIntoChannel(channel: Channel) {
        channel.write(Unpooled.wrappedBuffer(buffer))
    }

    override fun totalSize(): Int {
        return size()
    }
}