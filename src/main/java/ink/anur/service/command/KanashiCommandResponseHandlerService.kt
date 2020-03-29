package ink.anur.service.command

import ink.anur.core.common.AbstractRequestMapping
import ink.anur.core.request.RequestProcessCentreService
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.pojo.command.KanashiCommandDto
import ink.anur.pojo.command.KanashiCommandResponse
import ink.anur.pojo.enumerate.RequestTypeEnum
import ink.anur.pojo.log.KanashiCommand
import ink.anur.pojo.log.common.CommandTypeEnum
import ink.anur.pojo.log.common.StrApiTypeEnum
import ink.anur.pojo.log.common.TransactionTypeEnum
import io.netty.channel.Channel
import java.nio.ByteBuffer
import java.util.concurrent.CountDownLatch

/**
 * Created by Anur IjuoKaruKas on 2020/3/29
 */
@NigateBean
class KanashiCommandResponseHandlerService : AbstractRequestMapping() {

    @NigateInject
    private lateinit var requestProcessCentreService: RequestProcessCentreService

    /**
     * 返回的结果会保存在这个 map 里面
     */
    private val responseMap = mutableMapOf<Long, KanashiCommandResponse>()

    /**
     * 通知调用线程唤醒的 map
     */
    private val notifyMap = mutableMapOf<Long, CountDownLatch>()


    override fun typeSupport(): RequestTypeEnum {
        return RequestTypeEnum.COMMAND_RESPONSE
    }

    override fun handleRequest(fromServer: String, msg: ByteBuffer, channel: Channel) {
        val kanashiCommandResponse = KanashiCommandResponse(msg)

        synchronized(this) {
            responseMap[kanashiCommandResponse.msgTime] = kanashiCommandResponse
            notifyMap.remove(kanashiCommandResponse.msgTime)?.countDown()
        }
    }

    fun acquire(kanashiCommandDto: KanashiCommandDto): KanashiCommandResponse {
        val timeMillis = kanashiCommandDto.getTimeMillis()

        var reCall = false
        val waitForResponse = CountDownLatch(1)

        synchronized(this) {
            if (notifyMap[timeMillis] != null) {
                kanashiCommandDto.resetTimeMillis()
                reCall = true
            } else {
                notifyMap[timeMillis] = waitForResponse
            }
        }
        return if (reCall) {
            acquire(kanashiCommandDto)
        } else {
            requestProcessCentreService.sendToServer(kanashiCommandDto)
            waitForResponse.await()
            responseMap.remove(timeMillis)!!
        }
    }
}