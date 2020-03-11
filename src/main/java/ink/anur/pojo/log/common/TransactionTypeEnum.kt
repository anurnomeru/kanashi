package ink.anur.pojo.log.common

import ink.anur.exception.UnSupportTransException

/**
 * Created by Anur IjuoKaruKas on 2019/9/18
 */
enum class TransactionTypeEnum(val byte: Byte) {

    /** 这个类型表示为，此操作短事务*/
    SHORT(-128),

    /** 这个类型表示为，此操作长事务*/
    LONG(-127);

    companion object {
        private val MAPPER = HashMap<Byte, TransactionTypeEnum>()

        init {
            for (value in TransactionTypeEnum.values()) {
                MAPPER[value.byte] = value
            }
        }

        fun map(byte: Byte): TransactionTypeEnum {
            return MAPPER[byte] ?: throw UnSupportTransException()
        }
    }
}