package ink.anur.service

import ink.anur.struct.enumerate.OperationTypeEnum
import ink.anur.core.coordinator.common.AbstractRequestMapping
import io.netty.channel.Channel
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2020/2/28
 *
 * 专门处理心跳的处理器
 */
class HeartbeatHandleService : AbstractRequestMapping() {
    override fun typeSupport(): OperationTypeEnum {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun handleRequest(fromServer: String, msg: ByteBuffer, channel: Channel) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

}