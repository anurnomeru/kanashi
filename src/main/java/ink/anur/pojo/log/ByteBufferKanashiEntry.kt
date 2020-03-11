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
 *  commandType  + operationType  +  valueSize + value.
 */
class ByteBufferKanashiEntry(val byteBuffer: ByteBuffer) {

    /**
     * 对整个 ByteBufferKanashiEntry 大小的预估
     */
    val expectedSize: Int = byteBuffer.limit()

    var operateTypeSet = false

    /**
     *设置 operateType
     */
    fun setOperateType(operateType: OperateType) {
        byteBuffer.mark()
        byteBuffer.position(OperateTypeOffset)
        byteBuffer.put(operateType.byte)
        byteBuffer.reset()
        operateTypeSet = true
    }

    /**
     * 操作类型
     */
    fun getOperateType() = Companion.OperateType.map(byteBuffer.get(OperateTypeOffset))

    /**
     * STR操作还是LIST还是什么别的
     */
    fun getCommandType() = CommandTypeEnum.map(byteBuffer.get(CommandTypeOffset))

    @Deprecated("不要返回字符串")
    fun getValue(): String {
        byteBuffer.mark()
        byteBuffer.position(ValueSizeOffset)
        val mainParamSize = byteBuffer.getInt()

        val valueArray = ByteArray(mainParamSize)
        byteBuffer.get(valueArray)
        byteBuffer.reset()
        return String(valueArray)
    }

    companion object {

        val NONE: ByteBufferKanashiEntry

        init {
            val allocate = ByteBuffer.allocate(1)
            NONE = ByteBufferKanashiEntry(allocate)
        }

        // 理论上key 可以支持到很大，但是一个key 2g = = 玩呢？

        /**
         * 与 kanashiCommand 同义
         */
        const val CommandTypeOffset = 0
        const val CommandTypeLength = 1

        /**
         * 标记此值是否被删除
         */
        const val OperateTypeOffset = CommandTypeOffset + CommandTypeLength
        const val OperateTypeLength = 1

        /**
         * value 长度标识
         */
        const val ValueSizeOffset = OperateTypeOffset + OperateTypeLength
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
}