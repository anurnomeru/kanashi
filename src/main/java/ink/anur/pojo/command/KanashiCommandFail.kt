package ink.anur.pojo.command

import ink.anur.pojo.common.AbstractTimedStruct
import ink.anur.pojo.enumerate.RequestTypeEnum
import ink.anur.pojo.log.ByteBufferKanashiEntry
import io.netty.channel.Channel
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2020/3/28
 *
 * 成功的回复
 */
class KanashiCommandFail : AbstractTimedStruct {

    constructor() {
        init(ByteBuffer.allocate(OriginMessageOverhead), RequestTypeEnum.COMMAND_FAIL)
        buffer!!.flip()
    }

    constructor(byteBuffer: ByteBuffer) {
        byteBuffer.position(0)
        byteBuffer.limit(OriginMessageOverhead)
        buffer = byteBuffer.slice()
    }

    override fun writeIntoChannel(channel: Channel) {
        channel.write(buffer)
        channel.flush()
    }

    override fun totalSize(): Int {
        return size()
    }
}