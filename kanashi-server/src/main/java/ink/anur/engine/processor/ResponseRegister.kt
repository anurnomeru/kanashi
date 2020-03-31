package ink.anur.engine.processor


/**
 * Created by Anur IjuoKaruKas on 2020/3/31
 *
 * 这个类用于告知数据引擎，这条数据需要进行回复
 */
class ResponseRegister(
    /**
     * 消息时间，只是用于请求方辨别是哪个请求的回复用
     */
    val msgTime: Long,
    /**
     * 是否由客户端直接请求过来，如果是，要进行回复
     */
    val fromClient: String) {
}