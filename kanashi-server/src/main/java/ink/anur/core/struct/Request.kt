package ink.anur.core.struct

import ink.anur.struct.enumerate.RequestTypeEnum
import io.netty.channel.Channel
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2019/8/7
 */
class Request(val msg: ByteBuffer, val typeEnum: RequestTypeEnum, val channel: Channel?){
}