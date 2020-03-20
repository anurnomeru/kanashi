package ink.anur.engine.prelog

import ink.anur.pojo.log.common.GenerationAndOffset
import ink.anur.engine.StoreEngineFacadeService
import ink.anur.engine.log.LogService
import ink.anur.exception.LogException
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.inject.NigatePostConstruct
import ink.anur.log.operationset.ByteBufferOperationSet
import ink.anur.log.prelog.ByteBufPreLog
import ink.anur.log.prelog.PreLogMeta
import ink.anur.mutex.ReentrantReadWriteLocker
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentSkipListMap

/**
 * Created by Anur IjuoKaruKas on 2019/7/12
 *
 * 负责写入预日志，并操作将预日志追加到日志中
 */
@NigateBean
class ByteBufPreLogService : ReentrantReadWriteLocker() {

    @NigateInject
    private lateinit var logService: LogService

    @NigateInject
    private lateinit var storeEngineFacadeService: StoreEngineFacadeService

    private val preLog: ConcurrentSkipListMap<Long, ByteBufPreLog> = ConcurrentSkipListMap()

    private val logger = LoggerFactory.getLogger(ByteBufPreLogService::class.java)

    /**
     * 当前已经提交的 offset
     */
    private var commitOffset: GenerationAndOffset? = null

    /**
     * 预日志最后一个 offset
     */
    private var preLogOffset: GenerationAndOffset? = null

    @NigatePostConstruct(dependsOn = "LogService")
    private fun init() {
        this.commitOffset = logService.getInitial()
        this.preLogOffset = commitOffset
        logger.info("预日志初始化成功，预日志将由 {} 开始", commitOffset!!.toString())
    }

    /**
     * 供 leader 写入使用, 仅供降级为 follower 时删除未提交的 gao
     */
    fun cover(GAO: GenerationAndOffset) {
        writeLocker {
            commitOffset = GAO
            preLogOffset = GAO
        }
    }

    /**
     * 获取当前副本同步到的最新的 preLog GAO
     */
    fun getPreLogGAO(): GenerationAndOffset {
        return readLockSupplierCompel { preLogOffset!! }
    }

    /**
     * 获取当前副本同步到的最新的 commit GAO
     */
    fun getCommitGAO(): GenerationAndOffset {
        return readLockSupplierCompel { commitOffset!! }
    }

    /**
     * 此添加必须保证一次调用中，ByteBufferOperationSet 所有的操作日志都在同一个世代，实际上也确实如此
     */
    fun append(generation: Long, byteBufferOperationSet: ByteBufferOperationSet) {
        writeLocker {
            /* 简单检查 */
            if (generation < preLogOffset!!.generation) {
                logger.error("追加到预日志的日志 generation {} 小于当前预日志 generation {}，追加失败！", generation, preLogOffset!!.getGeneration())
                return@writeLocker
            }

            val byteBufPreLogOperated = preLog.compute(generation) { _, byteBufPreLog ->
                byteBufPreLog ?: ByteBufPreLog(generation)
            }

            val iterator = byteBufferOperationSet.iterator()
            var lastOffset = -1L

            while (iterator.hasNext()) {
                val oao = iterator.next()

                val oaoOffset = oao.offset

                if (GenerationAndOffset(generation, oaoOffset) <= preLogOffset!!) {
                    logger.error("追加到预日志的日志 offset $oaoOffset 小于当前预日志 offset ${preLogOffset!!.offset}，追加失败！！")
                    break
                }

                byteBufPreLogOperated!!.append(oao.logItem, oaoOffset)
                lastOffset = oaoOffset
            }

            if (lastOffset != -1L) {
                val before = preLogOffset
                preLogOffset = GenerationAndOffset(generation, lastOffset)
                logger.debug("本地追加了预日志，由 {} 更新至 {}", before.toString(), preLogOffset.toString())
            }
        }
    }

    /**
     * follower 将此 offset 往后的数据都从内存提交到本地
     */
    fun commit(GAO: GenerationAndOffset) {
        writeLocker {
            // 先与本地已经提交的记录做对比，只有大于本地副本提交进度时才进行commit
            val compareResult = GAO.compareTo(commitOffset)

            // 需要提交的进度小于等于preLogOffset
            if (compareResult <= 0) {
                return@writeLocker
            } else {
                val canCommit = readLockSupplierCompel { if (GAO > preLogOffset) preLogOffset else GAO }

                if (canCommit == commitOffset) {
                    logger.debug("收到来自 Leader 节点的有效 commit 请求，本地预日志最大为 {} ，故可提交到 {} ，但本地已经提交此进度。", preLogOffset.toString(), canCommit!!.toString())
                } else {
                    logger.debug("收到来自 Leader 节点的有效 commit 请求，本地预日志最大为 {} ，故可提交到 {}", preLogOffset.toString(), canCommit!!.toString())

                    val preLogMeta = getBefore(canCommit) ?: throw LogException("有bug请注意排查！！，不应该出现这个情况")

                    // 追加到磁盘
                    logService.append(preLogMeta, canCommit.generation, preLogMeta.startOffset, preLogMeta.endOffset)

                    // 强制刷盘
                    logService.activeLog().flush(preLogMeta.endOffset)

                    logger.debug("本地预日志 commit 进度由 {} 更新至 {}", commitOffset.toString(), canCommit.toString())
                    commitOffset = canCommit
                    discardBefore(canCommit)
                    storeEngineFacadeService.coverCommittedProjectGenerationAndOffset(canCommit)
                }
            }
        }
    }

    /**
     * 获取当前这一条之前的预日志（包括这一条）
     */
    private fun getBefore(GAO: GenerationAndOffset): PreLogMeta? {
        return this.readLockSupplier {
            val gen = GAO.generation
            val offset = GAO.offset
            val head = preLog.headMap(gen, true)

            if (head == null || head.size == 0) {
                throw LogException("获取预日志时：世代过小或者此世代还未有预日志")
            }

            val byteBufPreLog = head.firstEntry()
                .value

            byteBufPreLog.getBefore(offset)
        }
    }

    /**
     * 丢弃掉一些预日志消息（批量丢弃，包括这一条）
     */
    private fun discardBefore(GAO: GenerationAndOffset) {
        this.writeLockSupplier {
            val gen = GAO.generation
            val offset = GAO.offset
            val head = preLog.headMap(gen, true)

            if (head == null || head.size == 0) {
                throw LogException("获取预日志时：世代过小或者此世代还未有预日志")
            }

            val byteBufPreLog = head.firstEntry()
                .value
            if (byteBufPreLog.discardBefore(offset)) preLog.remove(byteBufPreLog.generation)
        }
    }
}