package ink.anur.engine.persistant


/**
 * Created by Anur IjuoKaruKas on 2019/12/5
 */
object FileKanashiEntryConstant {

    const val SizeOffset = 0
    const val SizeLength = 4

    const val KeySizeOffset = SizeOffset + SizeLength
    const val KeySizeLength = 4

    const val KeyOffset = KeySizeOffset + KeySizeLength
    const val minFilekanashiEntryOverHead = KeyOffset

    /**
     * 对整个 ByteBufferKanashiEntry 大小的预估
     */
    fun getExpectedSizeOverHead(key: String): Int = minFilekanashiEntryOverHead + key.toByteArray().size
}