package ink.anur.core.struct

import ink.anur.common.struct.enumerate.OperationTypeEnum
import io.netty.channel.Channel
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2019/8/7
 */
class CoordinateRequest(val msg: ByteBuffer, val typeEnum: OperationTypeEnum, val channel: Channel?){
}