package ink.anur.log.prelog

import ink.anur.log.common.LogItemAndOffset
import ink.anur.mutex.ReentrantLocker
import ink.anur.pojo.log.base.LogItem
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentNavigableMap
import java.util.concurrent.ConcurrentSkipListMap

/**
 * Created by Anur IjuoKaruKas on 2019/7/12
 *
 * 内存版预日志，主要用于子节点不断从leader节点同步预消息
 */
class ByteBufPreLog(val generation: Long) : ReentrantLocker() {

    private val logger = LoggerFactory.getLogger(ByteBufPreLog::class.java)

    private val preLog: ConcurrentNavigableMap<Long, LogItemAndOffset> = ConcurrentSkipListMap()

    /**
     * 将消息添加到内存中
     */
    fun append(logItem: LogItem, offset: Long) {
        preLog[offset] = LogItemAndOffset(logItem, offset)
    }

    /**
     * 获取此消息之前的消息（包括 targetOffset 这一条）
     */
    fun getBefore(targetOffset: Long): PreLogMeta? {
        val result = preLog.headMap(targetOffset, true)
        return if (result.size == 0) null else PreLogMeta(result.firstKey(), result.lastKey(), result.values)
    }

    /**
     * 丢弃之前的消息们
     */
    fun discardBefore(targetOffset: Long): Boolean {
        val discardMap = preLog.headMap(targetOffset, true)
        val count = discardMap.size
        for (key in discardMap.keys) {
            preLog.remove(key)
        }
        logger.debug("丢弃世代 {} 小于等于 {} 的共 {} 条预日志", generation, targetOffset, count)
        return preLog.size == 0
    }

}