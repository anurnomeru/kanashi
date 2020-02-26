package ink.anur.common.struct.common

import ink.anur.common.struct.enumerate.OperationTypeEnum
import ink.anur.exception.ByteBufferValidationException
import ink.anur.util.ByteBufferUtil
import io.netty.channel.Channel
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2020/2/22
 *
 * 一个 基础的数据 由以下部分组成：
 *
 * 　4　   +   4    + ...（子类自由扩展）
 * CRC32  +  type  + ...（子类自由扩展）
 *
 * 所有的指令都满足 4位CRC + 4位类型
 */
abstract class AbstractStruct {

    /**
     *  4字节 crc + 4字节类型 + 内容
     */
    companion object {
        val CrcOffset = 0
        val CrcLength = 4
        val TypeOffset = CrcOffset + CrcLength
        val TypeLength = 4


        val truely: Byte = 1
    }

    fun translateToByte(boolean: Boolean): Byte {
        return if (boolean) {
            1
        } else {
            0
        }
    }

    fun translateToBool(byte: Byte): Boolean {
        return byte == truely
    }

    // =================================================================

    protected var buffer: ByteBuffer? = null

    fun getByteBuffer(): ByteBuffer? {
        return buffer
    }

    fun size(): Int {
        return buffer!!.limit()
    }

    fun ensureValid() {
        val stored = checkSum()
        val compute = computeChecksum()
        if (stored != compute) {
            throw ByteBufferValidationException()
        }
    }

    fun checkSum(): Long {
        return ByteBufferUtil.readUnsignedInt(buffer, CrcOffset)
    }

    fun computeChecksum(): Long {
        return ByteBufferUtil.crc32(buffer!!.array(), buffer!!.arrayOffset() + TypeOffset, buffer!!.limit() - TypeOffset)
    }

    fun getOperationTypeEnum(): OperationTypeEnum {
        return OperationTypeEnum.parseByByteSign(buffer!!.getInt(TypeOffset))
    }

    /**
     * 如何写入 Channel
     */
    abstract fun writeIntoChannel(channel: Channel)

    /**
     * 真正的 size，并不局限于维护的 buffer
     */
    abstract fun totalSize(): Int
}