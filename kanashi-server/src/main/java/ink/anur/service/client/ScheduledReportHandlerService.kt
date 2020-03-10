package ink.anur.service.client

import ink.anur.core.common.AbstractRequestMapping
import ink.anur.inject.NigateBean
import ink.anur.pojo.client.ScheduledReport
import ink.anur.pojo.enumerate.RequestTypeEnum
import io.netty.channel.Channel
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2020/3/10
 */
@NigateBean
class ScheduledReportHandlerService : AbstractRequestMapping() {

    override fun typeSupport(): RequestTypeEnum {
        return RequestTypeEnum.SCHEDULED_REPORT
    }

    override fun handleRequest(fromServer: String, msg: ByteBuffer, channel: Channel) {
        val scheduledReport = ScheduledReport(msg)
        for (simpleScheduledMeta in scheduledReport.simpleScheduledList) {
            println()
        }
    }
}