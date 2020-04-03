package ink.anur.engine.memory

import ink.anur.debug.Debugger
import ink.anur.engine.persistant.FileKanashiEntryConstant
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.pojo.log.ByteBufferKanashiEntry

/**
 * Created by Anur IjuoKaruKas on 2019/10/23
 *
 * 内存部分的lsm树
 */
@NigateBean
class MemoryLSMService {

    private val logger = Debugger(this.javaClass)

    companion object {
        /**
         * 一个块 block 为 4Kb，假定平均一个元素为 64 - 128 byte，所以平均一下能存 1024 个 key
         */
        private const val FullMemoryAccess = Int.MAX_VALUE
//        private const val FullMemoryAccess = (1024 * 4) * 1024 * 16
    }

    /**
     * 责任链第一个lsm容器
     */
    @NigateInject
    private lateinit var firstChain: MemoryLSMChain


    private var chainCount = 1

    /**
     * 通过责任链去获取到数据
     */
    fun get(key: String): ByteBufferKanashiEntry? = firstChain.get(key)

    /**
     * compute，并更新空间
     *
     * todo 暂时不启用 lsm 因为还没有写lsm的打算
     */
    fun put(key: String, entry: ByteBufferKanashiEntry) {
        val expectedSizeOverHead = FileKanashiEntryConstant.getExpectedSizeOverHead(key)
        val entryExpectedSize = entry.expectedSize
        val expectedSize = expectedSizeOverHead + entryExpectedSize

        when {
            /*
             * 因 kanashiEntry 过大， 单 k-v 映射  ->  一个块
             * 故将此数据单独存到一个 MemoryLSM中，并位列当前lsm树之后
             */
            expectedSize > FullMemoryAccess -> {
                val memoryLSMChain = MemoryLSMChain()
                memoryLSMChain.memoryAssess = expectedSize
                memoryLSMChain.dataKeeper[key] = entry

                memoryLSMChain.nextChain = firstChain.nextChain
                firstChain.nextChain = memoryLSMChain

                if (firstChain.dataKeeper.containsKey(key)) {
                    val remove = firstChain.dataKeeper.remove(key)!!
                    firstChain.memoryAssess -= (expectedSizeOverHead + remove.expectedSize)
                }
                chainCount++

                logger.info("由于 kanashiEntry 过大，MemoryLSMService 将为其单独分配一个 block，已进行扩容，现拥有 {} 个 block", chainCount)
            }
            /*
             * 如果达到阈值，则创建新的lsm块
             */
            expectedSize + firstChain.memoryAssess > FullMemoryAccess -> {
                val memoryLSMChain = MemoryLSMChain()
                memoryLSMChain.memoryAssess = expectedSize
                memoryLSMChain.dataKeeper[key] = entry

                memoryLSMChain.nextChain = firstChain
                firstChain = memoryLSMChain

                chainCount++
                logger.info("在插入新 kanashiEntry size[{}] 后，block 大小 [{}] 将超过阈值 {}，" +
                    " MemoryLSMService 将新增一个 block，已进行扩容，现拥有 {} 个 block", expectedSize, firstChain.nextChain!!.memoryAssess, FullMemoryAccess, chainCount)
            }
            /*
             * 普通情况
             */
            else -> {
                firstChain.dataKeeper.compute(key) { _, v ->
                    v?.also {
                        firstChain.memoryAssess -= (expectedSizeOverHead + v.expectedSize)
                    }
                    firstChain.memoryAssess += expectedSize
                    entry
                }
            }
        }
    }
}