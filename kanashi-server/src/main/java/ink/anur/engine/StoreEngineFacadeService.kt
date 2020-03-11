package ink.anur.engine

import ink.anur.common.KanashiRunnable
import ink.anur.core.raft.gao.GenerationAndOffset
import ink.anur.debug.Debugger
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.log.common.OperationAndGAO
import ink.anur.log.persistence.CommitProcessManageService
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock


/**
 * Created by Anur IjuoKaruKas on 2019/10/10
 *
 * 存储引擎唯一对外开放的入口, 这使得存储引擎可以高内聚
 *
 * // todo 标记消费进度!!
 */
@NigateBean
class StoreEngineFacadeService : KanashiRunnable() {

    @NigateInject
    private lateinit var commitProcessManageService: CommitProcessManageService

    private val logger = Debugger(this::class.java)
    private val queue = LinkedBlockingQueue<OperationAndGAO>()
    private val lock = ReentrantLock()
    private val pauseLatch = lock.newCondition()

    override fun run() {
        // 启动锁
        lock.lock()
        pauseLatch.await()
        lock.unlock()

        logger.error("| - 存储引擎控制中心 - | 已经启动")
        while (true) {
            val take = queue.take()

            try {
                blockCheckIter(take.GAO)
//                EngineDataFlowController.commandInvoke(take.operation) TODO
            } catch (e: Exception) {
                e.printStackTrace()
            }
        }
    }

    private val counter = AtomicInteger()

    /**
     * 检查是否需要阻塞
     */
    private fun blockCheckIter(Gao: GenerationAndOffset) {
        val latestCommitted = commitProcessManageService.load()
        if (latestCommitted != GenerationAndOffset.INVALID && Gao > latestCommitted) {
            lock.lock()
            logger.error("| - 存储引擎控制中心 - | 已经消费到最新的提交进度 ${latestCommitted}，存储引擎将暂停等待唤醒")
            pauseLatch.await()
            logger.error("| - 存储引擎控制中心 - | 存储引擎被唤醒，将继续将消息存入存储引擎")
            lock.unlock()
            blockCheckIter(Gao)
        } else {
            if (counter.incrementAndGet() % 100000 == 0) logger.info("| - 存储引擎控制中心 - | 当前最新提交进度为： ${latestCommitted}，消费进度为：${Gao} ")
        }
    }

    /**
     * 继续消费
     */
    fun coverCommittedProjectGenerationAndOffset(Gao: GenerationAndOffset) {
        lock.lock()
        pauseLatch.signalAll()
        commitProcessManageService.cover(Gao)
        pauseLatch.signalAll()
        lock.unlock()
    }

    /**
     * 追加消息
     */
    fun append(oaGao: OperationAndGAO) {
        queue.put(oaGao)
    }
}
