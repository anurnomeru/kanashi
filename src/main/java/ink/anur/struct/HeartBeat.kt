package ink.anur.struct

import ink.anur.struct.common.AbstractTimedStruct
import io.netty.channel.Channel

/**
 * Created by Anur IjuoKaruKas on 2020/2/28
 */
class HeartBeat: AbstractTimedStruct() {
    override fun writeIntoChannel(channel: Channel) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun totalSize(): Int {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}