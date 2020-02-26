package ink.anur.core.raft

import ink.anur.common.KanashiRunnable
import ink.anur.config.ElectConfiguration
import ink.anur.config.InetSocketAddressConfiguration
import ink.anur.core.raft.gao.GenerationAndOffset
import ink.anur.core.raft.gao.GenerationAndOffsetService
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import org.slf4j.LoggerFactory
import javax.annotation.PostConstruct

/**
 * Created by Anur IjuoKaruKas on 2020/2/26
 *
 * 选举相关、raft核心控制代码
 */
@NigateBean
class RaftCenterController : KanashiRunnable() {

    @NigateInject
    private lateinit var electConfiguration: ElectConfiguration

    @NigateInject
    private lateinit var inetSocketAddressConfiguration: InetSocketAddressConfiguration

    @NigateInject
    private lateinit var generationAndOffsetService: GenerationAndOffsetService

    private val logger = LoggerFactory.getLogger(this::class.java)

    @PostConstruct
    private fun init() {
        logger.info("初始化选举控制器 ElectOperator，本节点为 {}", inetSocketAddressConfiguration.getLocalServerName())
        this.name = "RaftCenterController"
        this.start()
    }

    override fun run() {


        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}