package ink.anur.core.common

/**
 * Created by Anur IjuoKaruKas on 2020/3/8
 */
enum class RequestProcessType {

    /**
     * 仅仅只是发送，不需要回复，也不需要重发
     */
    SEND_ONCE,

    /**
     * 一直重发到收到回复为止
     */
    SEND_UNTIL_RESPONSE
}