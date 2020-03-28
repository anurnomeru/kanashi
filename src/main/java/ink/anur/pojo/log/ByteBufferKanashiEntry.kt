package ink.anur.pojo.log

import ink.anur.exception.UnSupportStorageTypeException
import ink.anur.pojo.log.common.CommandTypeEnum
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2019/12/4
 *
 * TODO kanashiCommand 和  ByteBufferKanashiEntry 共享底层 byteBuffer
 *
 * 负责将 KanashiEntry 写入磁盘
 *
 * 不包括 key 部分，则 KanashiEntry为以下部分组成：
 *
 *      1       +        1        +      4     + x
 *  commandType  + requestType  +  valueSize + value.
 */
class ByteBufferKanashiEntry(val byteBuffer: ByteBuffer) {

    companion object {

        val SENTINEL = allocateEmptyKanashiEntry()

        /**
         * 申请一个空值
         */
        fun allocateEmptyKanashiEntry(): ByteBufferKanashiEntry {
            val allocate = ByteBuffer.allocate(5)
            allocate.put(CommandTypeEnum.NONE.byte)
            allocate.putInt(0)
            allocate.flip()
            return ByteBufferKanashiEntry(allocate)
        }

        // 理论上key 可以支持到很大，但是一个key 2g = = 玩呢？

        /**
         * 与 kanashiCommand 同义
         */
        const val CommandTypeOffset = 0
        const val CommandTypeLength = 1

        /**
         * value 长度标识
         */
        const val ValueSizeOffset = CommandTypeOffset + CommandTypeLength
        const val ValueSizeLength = 4

        const val ValueOffset = ValueSizeOffset + ValueSizeLength

        /**
         * DISABLE 代表这个值被删掉了
         */
        enum class OperateType(val byte: Byte) {
            ENABLE(0),
            DISABLE(1);

            companion object {
                private val MAPPER = HashMap<Byte, OperateType>()

                init {
                    for (value in ByteBufferKanashiEntry.Companion.OperateType.values()) {
                        MAPPER[value.byte] = value
                    }
                }

                fun map(byte: Byte): OperateType {
                    return MAPPER[byte] ?: throw UnSupportStorageTypeException()
                }
            }
        }
    }

    /**
     * 对整个 ByteBufferKanashiEntry 大小的预估
     */
    val expectedSize: Int = byteBuffer.limit()

    /**
     * STR操作还是LIST还是什么别的
     */
    fun getCommandType() = CommandTypeEnum.map(byteBuffer.get(CommandTypeOffset))

    /**
     * 是否是一个被删除的值
     */
    fun isDelete(): Boolean = getCommandType() == CommandTypeEnum.NONE

    fun getValueString(): String {
        byteBuffer.mark()
        byteBuffer.position(ValueSizeOffset)
        val mainParamSize = byteBuffer.getInt()

        val valueArray = ByteArray(mainParamSize)
        byteBuffer.get(valueArray)
        byteBuffer.reset()
        return String(valueArray)
    }
}