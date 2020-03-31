package ink.anur.pojo.log

import ink.anur.exception.KanashiException
import ink.anur.pojo.log.common.CommandTypeEnum
import ink.anur.pojo.log.common.StrApiTypeEnum
import ink.anur.pojo.log.common.TransactionTypeEnum
import java.nio.ByteBuffer
import javax.annotation.concurrent.NotThreadSafe

/**
 * Created by Anur IjuoKaruKas on 2019/9/17
 *
 * 对应一个最基础最基础的操作，这个“操作”代表操作数据库的操作
 *
 * 一个 Command 由以下部分组成：
 *
 * 　8　   +    1    +       1       +        1        +      4 + x ...
 * trxId  +   api   +  commandType  + transactionSign +  valueSize + value...
 */
@NotThreadSafe
class KanashiCommand(val content: ByteBuffer) {

    companion object {

        /**
         * 代表不需要事务（短查询）
         */
        const val NON_TRX = Long.MAX_VALUE

        const val TrxIdOffset = 0
        const val TrxIdLength = 8

        const val ApiOffset = TrxIdOffset + TrxIdLength
        const val ApiLength = 1

        const val CommandTypeOffset = ApiOffset + ApiLength
        const val CommandTypeLength = 1

        const val TransactionSignOffset = CommandTypeOffset + CommandTypeLength
        const val TransactionSignLength = 1

        const val ValuesSizeOffset = TransactionSignOffset + TransactionSignLength

        /**
         * 表明参数长度，四个字节 values size 可传多参数，格式为 size+value size+value
         *
         * 第一个参数为真正 KanashiEntry的值
         */
        const val ValuesSizeLength = 4

        fun generator(trxId: Long?, transactionSign: TransactionTypeEnum, commandType: CommandTypeEnum, api: Byte, vararg values: String = arrayOf("")): KanashiCommand {
            if (values.isEmpty()) {
                throw KanashiException("不允许生成值为空数组的命令！至少要传一个含有空字符串的数组")
            }

            val valuesSizeLengthTotal = values.size * ValuesSizeLength
            val valuesByteArr = values.map { it.toByteArray() }
            val bb = ByteBuffer.allocate(
                ValuesSizeOffset
                    + valuesSizeLengthTotal
                    + valuesByteArr.map { it.size }.reduce { i1, i2 -> i1 + i2 })
            bb.putLong(trxId ?: NON_TRX)
            bb.put(api)
            bb.put(commandType.byte)
            bb.put(transactionSign.byte)
            valuesByteArr.forEach {
                it.also { arr ->
                    bb.putInt(arr.size)
                    bb.put(arr)
                }
            }
            bb.flip()
            return KanashiCommand(bb)
        }
    }

    val contentLength = content.limit()

    /**
     * 事务 id
     */
    var trxId = content.getLong(TrxIdOffset)

    /**
     * 是否开启了（长）事务
     */
    val transactionType = TransactionTypeEnum.map(content.get(TransactionSignOffset))

    /**
     * 操作类型，目前仅支持String类操作，第一版不要做那么复杂
     */
    val commandType = CommandTypeEnum.map(content.get(CommandTypeOffset))

    /**
     * 操作具体的api是哪个，比如增删改查之类的，每个操作类型都有自己的 api 类型，比如 字符串类型 str 的在 StrApiTypeEnum
     */
    val api = content.get(ApiOffset)

    /**
     * 获取非第一个参数的额外参数们，第一个参数放在 value 里面
     */
    val extraParams: MutableList<String> = mutableListOf()

    /**
     * 通过 kanashiCommand 来生成一个 ByteBufferKanashiEntry
     *
     * 调用后，limit会发生变化，第一个参数之后的数据将丢失
     */
    val kanashiEntry: ByteBufferKanashiEntry

    /**
     * 只有查询指令可以对从节点操作
     */
    val isQueryCommand: Boolean

    init {
        content.mark()
        content.position(ValuesSizeOffset)
        val mainParamSize = content.getInt()

        val kanashiEntryFrom = TransactionSignOffset
        val kanashiEntryTo = ValuesSizeOffset + ValuesSizeLength + mainParamSize

        // 首先取出额外参数
        content.position(kanashiEntryTo)
        while (content.position() < contentLength) {
            val param = ByteArray(content.getInt())
            content.get(param)
            extraParams.add(String(param))
        }

        // 其次取出 kanashiEntry
        content.position(kanashiEntryFrom)
        content.limit(kanashiEntryTo)
        kanashiEntry = ByteBufferKanashiEntry(content.slice())

        content.reset()

        // 判断是否查询指令
        isQueryCommand = when (commandType) {
            CommandTypeEnum.STR -> {
                when (api) {
                    StrApiTypeEnum.SELECT -> true
                    else -> false
                }
            }
            else -> false
        }
    }


    override fun toString(): String {
        return "KanashiEntry{" +
            "trxId='" + trxId + '\'' +
            ", type='" + commandType + '\'' +
            ", api='" + api + '\'' +
            "}"
    }
}