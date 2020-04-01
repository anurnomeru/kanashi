package ink.anur.service.command

import ink.anur.core.common.AbstractRequestMapping
import ink.anur.core.request.RequestProcessCentreService
import ink.anur.inject.NigateAfterBootStrap
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.pojo.command.KanashiCommandDto
import ink.anur.pojo.command.KanashiCommandResponse
import ink.anur.pojo.enumerate.RequestTypeEnum
import io.netty.channel.Channel
import java.nio.ByteBuffer
import java.util.concurrent.CountDownLatch

/**
 * Created by Anur IjuoKaruKas on 2020/3/29
 */
@NigateBean
class KanashiCommandResponseHandlerService : AbstractRequestMapping() {

    private val nigateActivateLatch = CountDownLatch(1)

    @NigateInject
    private lateinit var requestProcessCentreService: RequestProcessCentreService


    @NigateAfterBootStrap
    private fun afterBootStrap() {
        nigateActivateLatch.countDown()
    }

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
        nigateActivateLatch.await()
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
        return if (reCall) {// 针对时间重复的要重新递归请求一次
            acquire(kanashiCommandDto)
        } else {
            if (requestProcessCentreService.sendToServer(kanashiCommandDto)) {
                waitForResponse.await()
                responseMap.remove(timeMillis)!!
            } else {
                // TODO 做有限次数的尝试
                Thread.sleep(200L)
                return acquire(kanashiCommandDto)
            }
        }
    }
}