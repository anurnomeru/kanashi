package ink.anur.engine.memory

import ink.anur.debug.Debugger
import ink.anur.engine.common.VerAndKanashiEntry
import ink.anur.engine.common.VerAndKanashiEntryWithKeyPair
import ink.anur.engine.processor.DataHandler
import ink.anur.exception.MemoryMVCCStorageUnCommittedPartException
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.pojo.log.ByteBufferKanashiEntry
import ink.anur.pojo.log.common.GenerationAndOffset
import java.util.*

/**
 * Created by Anur IjuoKaruKas on 2019/10/11
 *
 * 内存存储实现，支持 mvcc （未提交部分）
 */
@NigateBean
class MemoryMVCCStorageUnCommittedPart {

    private val logger = Debugger(this.javaClass)

    @NigateInject
    private lateinit var memoryMVCCStorageCommittedPart: MemoryMVCCStorageCommittedPart

    private val treeMap = TreeMap<String, VerAndKanashiEntry>()

    /**
     * 查找未提交的 kanashiEntry，传入的 trxId 必须为此 key 持有的那个事务id才可以查到，否则
     * 由于隔离性，未提交的不可以查出来
     */
    fun queryKeyInTrx(trxId: Long, key: String): ByteBufferKanashiEntry? {
        val verAndKanashiEntry = treeMap[key]
        if (verAndKanashiEntry != null && verAndKanashiEntry.trxId == trxId) {
            return verAndKanashiEntry.kanashiEntry
        }
        return null
    }

    /**
     * 将数据存入 unCommit 部分
     *
     * 在 Operate 将 事务下操作的键放入 treeMap
     */
    fun commonOperate(dataHandler: DataHandler) {
        val key = dataHandler.key
        val trxId = dataHandler.getTrxId()
        val kanashiEntry = dataHandler.getKanashiEntry()
        if (treeMap.containsKey(key) && treeMap[key]!!.trxId != trxId) {
            throw MemoryMVCCStorageUnCommittedPartException("mvcc uc部分出现了奇怪的bug，讲道理一个 key 只会对应一个 val，注意无锁控制 TrxFreeQueuedSynchronizer 是否有问题！")
        } else {
            logger.trace("事务 [{}] key [{}] 已经进入 un commit part", trxId, key)
            treeMap[key] = VerAndKanashiEntry(trxId, kanashiEntry)
        }
    }

    /**
     * 将数据推入 commit 部分
     *
     * 在 flush 则，将事务下的键塞入提交部分
     */
    fun flushToCommittedPart(trxId: Long, holdKeys: MutableSet<String>) {
        val verAndKanashiEntryWithKeyPairList = mutableListOf<VerAndKanashiEntryWithKeyPair>()
        for (holdKey in holdKeys) {
            treeMap[holdKey]?.also { verAndKanashiEntryWithKeyPairList.add(VerAndKanashiEntryWithKeyPair(holdKey, it)) }
        }
        if (verAndKanashiEntryWithKeyPairList.isNotEmpty()) {
            memoryMVCCStorageCommittedPart.flushTo(trxId, verAndKanashiEntryWithKeyPairList)
        }

        // 必须要先拿出来，存到 commit 的才可以删除，不然查询的时候可能会有疏漏
        holdKeys.forEach { treeMap.remove(it) }
    }

    /**
     * 将未提交的数据丢弃
     */
    fun discard(trxId: Long, holdKeys: MutableSet<String>) {
        logger.debug("事务 $trxId 已回滚")

        // 直接将所有的曾在这个事务里操作的key都移除掉
        holdKeys.forEach { treeMap.remove(it) }
    }
}
