package ink.anur.pojo.client

import ink.anur.pojo.common.AbstractTimedStruct
import ink.anur.pojo.enumerate.RequestTypeEnum
import io.netty.buffer.Unpooled
import io.netty.channel.Channel
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2020/3/10
 */
class ScheduledReport : AbstractTimedStruct {

    val simpleScheduledList = mutableListOf<SimpleScheduledMeta>()

    companion object {
        val SimpleScheduledLength = OriginMessageOverhead
        val SimpleScheduledLengthSize = 4

        val SimpleScheduledNameLength = 4
        val SimpleScheduledCronLength = 4
    }

    constructor()

    constructor(byteBuffer: ByteBuffer) {
        byteBuffer.position(SimpleScheduledLength)
        val SimpleScheduledLengthSize = byteBuffer.getInt()
        while (byteBuffer.position() < SimpleScheduledLength + 4 + SimpleScheduledLengthSize) {
            val nameLength = byteBuffer.getInt()
            val nameArr = ByteArray(nameLength)
            byteBuffer.get(nameArr)

            val cronLength = byteBuffer.getInt()
            val cronArr = ByteArray(cronLength)
            byteBuffer.get(cronArr)

            simpleScheduledList.add(SimpleScheduledMeta(nameArr, cronArr))
        }
    }

    var totalSize: Int? = null

    var simpleScheduledLength: Int? = null

    var prepared = false

    @Synchronized
    fun prepare() {
        if (!prepared) {
            val capacity = SimpleScheduledLength + 4 + SimpleScheduledLengthSize

            var simpleScheduledLength = 0
            for (meta in simpleScheduledList) {
                meta.prepareForBytes()
                simpleScheduledLength += meta.totalSize
            }
            this.simpleScheduledLength = simpleScheduledLength
            this.totalSize = capacity + simpleScheduledLength

            val byteBuffer = ByteBuffer.allocate(this.totalSize!!)
            init(byteBuffer, RequestTypeEnum.SCHEDULED_REPORT)
            byteBuffer.putInt(this.simpleScheduledLength!!)
            for (meta in simpleScheduledList) {
                byteBuffer.putInt(meta.nameArraySize)
                byteBuffer.put(meta.nameArray)
                byteBuffer.putInt(meta.cronArraySize)
                byteBuffer.put(meta.cronArray)
            }

            byteBuffer.flip()
        }
    }

    override fun writeIntoChannel(channel: Channel) {
        if (!prepared) {
            prepare()
        }
        val wrappedBuffer = Unpooled.wrappedBuffer(buffer)
        channel.write(wrappedBuffer)
    }

    override fun totalSize(): Int {
        if (!prepared) {
            prepare()
        }
        return this.totalSize!!
    }
}