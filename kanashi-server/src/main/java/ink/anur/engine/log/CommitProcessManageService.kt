package ink.anur.engine.log

import ink.anur.config.LogConfiguration
import ink.anur.pojo.log.common.GenerationAndOffset
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.inject.NigatePostConstruct
import ink.anur.engine.prelog.ByteBufPreLogService
import ink.anur.inject.Event
import ink.anur.inject.NigateListener
import ink.anur.mutex.ReentrantReadWriteLocker
import org.slf4j.LoggerFactory
import java.io.File
import java.io.RandomAccessFile
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel

/**
 * Created by Anur IjuoKaruKas on 2019/7/15
 *
 * 提交进度管理（担任过 leader 的节点需要用到）
 */
@NigateBean
class CommitProcessManageService : ReentrantReadWriteLocker() {

    @NigateInject
    private lateinit var logConfiguration: LogConfiguration

    @NigateInject
    private lateinit var byteBufPreLogService: ByteBufPreLogService

    @NigateInject
    private lateinit var logService: LogService

    private val logger = LoggerFactory.getLogger(this::class.java)

    private lateinit var offsetFile: File

    private var commitGAO: GenerationAndOffset? = null

    private lateinit var dir: File

    private lateinit var mmap: MappedByteBuffer

    @NigatePostConstruct(dependsOn = "LogService")
    private fun init() {
        offsetFile = File(logConfiguration.getBaseDir(), "commitOffset.temp")
        dir = File(logConfiguration.getBaseDir())
        dir.mkdirs()
        val createNewFile = offsetFile.createNewFile()
        val raf = RandomAccessFile(offsetFile, "rw")
        raf.setLength((8 + 8).toLong())

        mmap = raf.channel.map(FileChannel.MapMode.READ_WRITE, 0, (8 + 8).toLong())

        if (createNewFile) {
            logger.info("节点还未建立提交进度管理文件将创建，将创建默认进度管理文件")
            cover(GenerationAndOffset.INVALID)
        }

        load()
        discardInvalidMsg()
    }

    /**
     * 对于 leader 来说，由于不会写入 PreLog，故会持有未 commit 的消息
     *
     * 需要讲这些消息摈弃
     */
    @NigateListener(Event.CLUSTER_VALID)
    fun discardInvalidMsg() {
        if (commitGAO != null && commitGAO != GenerationAndOffset.INVALID) {
            logger.info("检测到本节点曾是 leader 节点，需摒弃部分未 Commit 的消息")
            logService.discardAfter(commitGAO!!)
            byteBufPreLogService.cover(commitGAO!!)
            cover(GenerationAndOffset.INVALID)
            commitGAO?.let { logger.info("摒弃完毕，当前 节点 GAO -> $it") }
        }
    }


    /**
     * TODO 初始值为 0 0 可能会有问题
     *
     * 加载 commitOffset.temp，获取集群提交进度（仅 leader 有效）
     */
    fun load(): GenerationAndOffset {
        writeLocker {
            if (commitGAO == null) {
                val gen = mmap.long
                val offset = mmap.long
                commitGAO = GenerationAndOffset(gen, offset)
                mmap.rewind()
            }
        }
        return commitGAO!!
    }

    /**
     * 覆盖提交进度
     */
    fun cover(GAO: GenerationAndOffset) {
        writeLocker {
            mmap.putLong(GAO.generation)
            mmap.putLong(GAO.offset)
            mmap.rewind()
            mmap.force()
            commitGAO = null
        }
    }
}