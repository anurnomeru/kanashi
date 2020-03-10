package ink.anur.service.client

import ink.anur.core.common.AbstractRequestMapping
import ink.anur.core.raft.ElectionMetaService
import ink.anur.inject.NigateBean
import ink.anur.pojo.client.ScheduledReport
import ink.anur.pojo.enumerate.RequestTypeEnum
import ink.anur.service.client.simple.SimpleSchedule
import io.netty.channel.Channel
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

/**
 * Created by Anur IjuoKaruKas on 2020/3/10
 */
@NigateBean
class ScheduledReportHandlerService : AbstractRequestMapping() {

    private lateinit var electionMetaService: ElectionMetaService

    private val SIMPLE_SCHEDULE_MAPPING = mutableMapOf<String, SimpleSchedule>()

    override fun typeSupport(): RequestTypeEnum {
        return RequestTypeEnum.SCHEDULED_REPORT
    }

    override fun handleRequest(fromServer: String, msg: ByteBuffer, channel: Channel) {
        val scheduledReport = ScheduledReport(msg)

        if (electionMetaService.isLeader()) {
            for (simpleScheduledMeta in scheduledReport.simpleScheduledList) {
                val name = simpleScheduledMeta.name
                val cron = simpleScheduledMeta.cron

                synchronized(name) {
                    if (SIMPLE_SCHEDULE_MAPPING[name] == null) {
                        SIMPLE_SCHEDULE_MAPPING[name] = SimpleSchedule(name, cron)
                    }
                    SIMPLE_SCHEDULE_MAPPING[name]!!.executeNodes
                }
            }
        }
    }
}