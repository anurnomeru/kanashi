package ink.anur.inject

import ink.anur.exception.NigateListenerException
import org.slf4j.LoggerFactory
import java.lang.reflect.Method

/**
 * Created by Anur IjuoKaruKas on 2020/3/8
 */
@NigateBean
class NigateListenerService {

    private val logger = LoggerFactory.getLogger(this::class.java)

    private val EVENT_POOL = mutableMapOf<Event, MutableList<ListenerContainer>>()

    fun registerListenEvent(bean: Any) {
        val methods = bean.javaClass.methods
        for (method in methods) {
            for (annotation in method.annotations) {
                if (annotation.annotationClass == NigateListener::class) {
                    if (method.annotatedParameterTypes.isNotEmpty()) {
                        throw NigateListenerException("bean ${bean.javaClass} 的监听方法 ${method.name} 不能有参数！")
                    }
                    annotation as NigateListener
                    val onEvent = annotation.onEvent
                    EVENT_POOL.compute(onEvent) { _, l ->
                        val list = l ?: mutableListOf()
                        list.add(ListenerContainer(bean, method))
                        return@compute list
                    }
                }
            }
        }
    }

    fun onEvent(onEvent: Event) {
        logger.debug("==> onEvent  | $onEvent |")
        logger
        val mutableList = EVENT_POOL[onEvent] ?: throw NigateListenerException("event $onEvent is not register")
        for (listenerContainer in mutableList) {
            listenerContainer.onEvent()
        }
    }

    inner class ListenerContainer(val bean: Any, val method: Method) {
        fun onEvent() {
            method.invoke(bean)
        }
    }
}