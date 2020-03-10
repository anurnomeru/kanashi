package ink.anur.service.client.simple

import ink.anur.common.struct.KanashiNode
import ink.anur.service.client.RunningInfo
import java.time.LocalDate

/**
 * Created by Anur IjuoKaruKas on 2020/3/10
 */
class SimpleSchedule(val name: String, val cron: String) {

    val executeNodes = mutableMapOf<String, RunningInfo>()

    /**
     * 此任务是否开启
     */
    val enable: Boolean = true

    /**
     * leader 节点有权限修改上述信息，所以此任务会有一个租期
     */
    val tenancy: LocalDate = LocalDate.MIN
}