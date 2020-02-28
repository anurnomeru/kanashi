package ink.anur.struct.enumerate

import ink.anur.struct.EmptyStruct
import ink.anur.struct.Register
import ink.anur.struct.RegisterResponse
import ink.anur.struct.Voting
import ink.anur.struct.common.AbstractStruct
import ink.anur.exception.KanashiException
import java.util.HashMap

/**
 * Created by Anur IjuoKaruKas on 2020/2/22
 */
enum class OperationTypeEnum(val byteSign: Int, val clazz: Class<out AbstractStruct>) {

    /**
     * 无类型
     */
    EMPTY_STRUCT(-1, EmptyStruct::class.java),

    /**
     * 协调从节点向主节点注册
     */
    REGISTER(10000, Register::class.java),

    /**
     * 协调从节点向主节点注册 的回复
     */
    REGISTER_RESPONSE(10001, RegisterResponse::class.java),

    /**
     * 进行投票
     */
    VOTING(10002, Voting::class.java),
    ;

    companion object {
        private val byteSignMap = HashMap<Int, OperationTypeEnum>()

        init {
            val unique = mutableSetOf<Int>()
            for (value in values()) {
                if (!unique.add(value.byteSign)) {
                    throw KanashiException("OperationTypeEnum 中，byteSign 不可重复。");
                }
                byteSignMap[value.byteSign] = value;
            }
        }

        fun parseByByteSign(byteSign: Int): OperationTypeEnum = byteSignMap[byteSign] ?: throw UnsupportedOperationException()
    }
}