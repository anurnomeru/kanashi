package ink.anur.common.struct.enumerate

import java.util.HashMap


/**
 * Created by Anur IjuoKaruKas on 2020/2/22
 */
enum class OperationTypeEnum {

    ;

    companion object {
        private val byteSignMap = HashMap<Int, OperationTypeEnum>()
        fun parseByByteSign(byteSign: Int): OperationTypeEnum {
            return byteSignMap[byteSign] ?: throw UnsupportedOperationException()
        }

    }
}