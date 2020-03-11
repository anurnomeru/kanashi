package ink.anur.pojo.enumerate

import ink.anur.pojo.EmptyStruct
import ink.anur.pojo.Register
import ink.anur.pojo.RegisterResponse
import ink.anur.pojo.coordinate.Voting
import ink.anur.pojo.common.AbstractStruct
import ink.anur.exception.KanashiException
import ink.anur.pojo.coordinate.Canvass
import ink.anur.pojo.HeartBeat
import ink.anur.pojo.server.FetchResponse
import ink.anur.pojo.server.Fetcher
import java.util.HashMap

/**
 * Created by Anur IjuoKaruKas on 2020/2/22
 */
enum class RequestTypeEnum(val byteSign: Int, val clazz: Class<out AbstractStruct>) {

    /**
     * 无类型
     */
    EMPTY_STRUCT(-1, EmptyStruct::class.java),

    /**
     * 心跳
     */
    HEAT_BEAT(9999, HeartBeat::class.java),

    /**
     * 协调从节点向主节点注册
     */
    REGISTER(10000, Register::class.java),

    /**
     * 协调从节点向主节点注册 的回复
     */
    REGISTER_RESPONSE(10001, RegisterResponse::class.java),

    /**
     * 进行拉票
     */
    CANVASS(10002, Canvass::class.java),

    /**
     * 进行投票
     */
    VOTING(10003, Voting::class.java),

    /**
     * 请求 fetch log
     */
    FETCH(20000, Fetcher::class.java),

    /**
     * fetch 结果
     */
    FETCH_RESPONSE(20001, FetchResponse::class.java),
    ;

    companion object {
        private val byteSignMap = HashMap<Int, RequestTypeEnum>()

        init {
            val unique = mutableSetOf<Int>()
            for (value in values()) {
                if (!unique.add(value.byteSign)) {
                    throw KanashiException("OperationTypeEnum 中，byteSign 不可重复。");
                }
                byteSignMap[value.byteSign] = value;
            }
        }

        fun parseByByteSign(byteSign: Int): RequestTypeEnum = byteSignMap[byteSign] ?: throw UnsupportedOperationException()
    }
}