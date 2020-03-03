package ink.anur.core.coordinator.common

import ink.anur.core.coordinator.core.CoordinateCentreService
import ink.anur.inject.Nigate
import ink.anur.inject.NigatePostConstruct

/**
 * Created by Anur IjuoKaruKas on 2020/2/25
 *
 * 定义一个消息如何消费的顶级接口
 */
abstract class AbstractRequestMapping : RequestMapping {

    @NigatePostConstruct
    fun init() {
        val msgCenterService = Nigate.getBeanByClass(CoordinateCentreService::class.java)
        msgCenterService.registerRequestMapping(this.typeSupport(), this)
    }
}