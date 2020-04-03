package ink.anur.engine

import ink.anur.common.KanashiExecutors
import ink.anur.common.KanashiRunnable
import ink.anur.core.raft.RaftCenterController
import ink.anur.pojo.log.common.GenerationAndOffset
import ink.anur.debug.Debugger
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.log.common.EngineProcessEntry
import ink.anur.engine.log.CommitProcessManageService
import ink.anur.engine.log.LogService
import ink.anur.engine.processor.DataHandler
import ink.anur.engine.processor.EngineExecutor
import ink.anur.inject.Event
import ink.anur.inject.NigateAfterBootStrap
import ink.anur.inject.NigateListener
import ink.anur.inject.NigatePostConstruct
import ink.anur.pojo.command.KanashiCommandDto
import ink.anur.pojo.log.KanashiCommand
import ink.anur.pojo.log.base.LogItem
import ink.anur.pojo.log.common.CommandTypeEnum
import ink.anur.pojo.log.common.StrApiTypeEnum
import ink.anur.pojo.log.common.TransactionTypeEnum
import java.util.Random
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.locks.ReentrantLock

/**
 * Created by Anur IjuoKaruKas on 2019/10/10
 *
 * 存储引擎唯一对外开放的入口, 这使得存储引擎可以高内聚
 */
@NigateBean
class StoreEngineFacadeService : KanashiRunnable() {

    @NigateInject
    private lateinit var commitProcessManageService: CommitProcessManageService

    @NigateInject
    private lateinit var storeEngineTransmitService: StoreEngineTransmitService

    @NigateInject
    private lateinit var logService: LogService

    @NigateInject
    private lateinit var raftCenterController: RaftCenterController

    private val logger = Debugger(this::class.java)
    private val queue = LinkedBlockingQueue<EngineProcessEntry>()
    private val lock = ReentrantLock()
    private val pauseLatch = lock.newCondition()

    @NigatePostConstruct
    private fun init() {
        this.start()
    }

//    @NigateListener(Event.RECOVERY_COMPLETE)
    private fun afterBootStrap() {

//        // 内存100w数据测试
//        val start = System.currentTimeMillis()
//        for (i in 0 until 1000000) {
//            val logItem = LogItem("$i",
//                KanashiCommand.generator(i.toLong(), TransactionTypeEnum.SHORT, CommandTypeEnum.STR, StrApiTypeEnum.SET, randomString(10, "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")))
//            storeEngineTransmitService.commandInvoke(EngineExecutor(DataHandler(EngineProcessEntry(logItem))))
//            if (i % 100000 == 0) {
//                println(i)
//            }
//        }
//        println("cost:")
//        println(System.currentTimeMillis() - start)


//        // 内存100w数据测试
//        val start = System.currentTimeMillis()
//        for (i in 0 until 1000000) {
//            val logItem = LogItem("$i",
//                KanashiCommand.generator(i.toLong(), TransactionTypeEnum.SHORT, CommandTypeEnum.STR, StrApiTypeEnum.SET, randomString(10, "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")))
//
//            logService.appendForLeader(raftCenterController.genGenerationAndOffset(), logItem)
//            if (i % 100000 == 0) {
//                println(i)
//            }
//        }
//        println("cost:")
//        println(System.currentTimeMillis() - start)

    }

    /**
     * 随机生成一个字符串，内容取自于range内指定的范围
     */
    fun randomString(length: Int, range: String): String {
        val sb = StringBuilder(length)
        if (range == "") {
            return ""
        }
        val r = Random()
        for (i in 0 until length) {
            val ch = range[r.nextInt(range.length)]
            sb.append(ch)
        }
        return sb.toString()
    }

    /**
     * 这里主要解决的矛盾是，判断何时可以往引擎写数据
     */
    override fun run() {
        KanashiExecutors.execute(Runnable {
            var currentNum = counter
            while (true) {
                Thread.sleep(1000)
                val nowNum = counter
                logger.debug("| - 存储引擎控制中心 - | 每秒流速 ->> ${nowNum - currentNum}")
                currentNum = nowNum
            }
        })

        logger.info("| - 存储引擎控制中心 - | 已经启动")
        while (true) {
            val take = queue.take()

            try {
                take.GAO?.let { blockCheckIter(it) }
                val engineExecutor = EngineExecutor(DataHandler(take))

                storeEngineTransmitService.commandInvoke(engineExecutor)
            } catch (e: Exception) {
                e.printStackTrace()
            }
        }
    }

    /**
     * 追加消息
     */
    fun append(engineProcessEntry: EngineProcessEntry) {
        queue.put(engineProcessEntry)
    }


    @Volatile
    private var counter: Int = 0

    /**
     * 检查是否需要阻塞，它只会消费已经提交的进度
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
            if (counter++ % 100000 == 0) logger.info("| - 存储引擎控制中心 - | 当前最新提交进度为： ${latestCommitted}，消费进度为：${Gao} ")
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
}
