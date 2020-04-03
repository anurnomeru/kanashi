package ink.anur.pojo.log

import ink.anur.common.struct.KanashiNode
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
class ByteBufferKanashiEntry {
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

    constructor(byteBuffer: ByteBuffer) {
        this.byteBuffer = byteBuffer
        expectedSize = byteBuffer.limit()
        commandType = CommandTypeEnum.map(byteBuffer.get(CommandTypeOffset))
    }

    constructor(commandType: CommandTypeEnum, byteBuffer: ByteBuffer) {
        val size = byteBuffer.limit()
        val allocate = ByteBuffer.allocate(size + ValueOffset)
        allocate.put(commandType.byte)
        allocate.putInt(size)
        allocate.put(byteBuffer)
        allocate.flip()
        this.byteBuffer = allocate
        expectedSize = allocate.limit()
        this.commandType = commandType
    }

    val byteBuffer: ByteBuffer

    /**
     * 对整个 ByteBufferKanashiEntry 大小的预估
     */
    val expectedSize: Int

    /**
     * STR操作还是LIST还是什么别的
     */
    val commandType: CommandTypeEnum

    /**
     * 是否是一个被删除的值
     */
    fun isDelete(): Boolean = commandType == CommandTypeEnum.NONE

    fun getValueString(): String {
        byteBuffer.mark()
        byteBuffer.position(ValueSizeOffset)
        val mainParamSize = byteBuffer.getInt()

        val valueArray = ByteArray(mainParamSize)
        byteBuffer.get(valueArray)
        byteBuffer.reset()
        return String(valueArray)
    }

    fun getValueLong(): Long {
        byteBuffer.mark()
        byteBuffer.position(ValueSizeOffset + 4)
        val result = byteBuffer.getLong()
        byteBuffer.reset()
        return result
    }

    fun getCluster(): List<KanashiNode> {
        byteBuffer.mark()
        byteBuffer.position(ValueSizeOffset + 4)
        val clusters = mutableListOf<KanashiNode>()
        while (byteBuffer.position() < byteBuffer.limit()) {
            val size = byteBuffer.getInt()
            val arr = ByteArray(size)
            byteBuffer.get(arr)
            val info = String(arr)
            val split = info.split(":")
            clusters.add(KanashiNode(split.get(0), split.get(1), split.get(2).toInt()))
        }
        return clusters
    }
}