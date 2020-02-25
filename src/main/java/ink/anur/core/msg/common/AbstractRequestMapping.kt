package ink.anur.core.msg.common

import ink.anur.core.msg.core.MsgCenterService
import ink.anur.inject.Nigate
import javax.annotation.PostConstruct

/**
 * Created by Anur IjuoKaruKas on 2020/2/25
 *
 * 定义一个消息如何消费的顶级接口
 */
abstract class AbstractRequestMapping : RequestMapping {

    @PostConstruct
    private fun init() {
        val msgCenterService = Nigate.getBeanByClass(MsgCenterService::class.java)
        msgCenterService.registerRequestMapping(this.typeSupport(), this)
    }
}