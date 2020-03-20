package ink.anur.engine.memory

import ink.anur.inject.NigateBean
import ink.anur.pojo.log.ByteBufferKanashiEntry
import java.util.*

/**
 * Created by Anur IjuoKaruKas on 2019/12/4
 */
@NigateBean
class MemoryLSMChain {

    /**
     * 存储空间评估
     */
    var memoryAssess: Int = 0

    var nextChain: MemoryLSMChain? = null

    val dataKeeper = TreeMap<String, ByteBufferKanashiEntry>()

    fun get(key: String): ByteBufferKanashiEntry? {
        return dataKeeper[key] ?: nextChain?.get(key)
    }
}