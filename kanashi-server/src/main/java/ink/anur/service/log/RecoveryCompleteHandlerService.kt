package ink.anur.service.log

import ink.anur.core.common.AbstractRequestMapping
import ink.anur.debug.Debugger
import ink.anur.engine.log.common.LogService
import ink.anur.engine.log.prelog.ByteBufPreLogService
import ink.anur.inject.Event
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.inject.NigateListenerService
import ink.anur.pojo.enumerate.RequestTypeEnum
import ink.anur.pojo.log.RecoveryComplete
import io.netty.channel.Channel
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2020/3/17
 *
 * 当集群收到 recovery complete 表示已经可以开始 fetch 日志了
 */
@NigateBean
class RecoveryCompleteHandlerService : AbstractRequestMapping() {

    @NigateInject
    private lateinit var byteBufPreLogService: ByteBufPreLogService

    @NigateInject
    private lateinit var logService: LogService

    @NigateInject
    private lateinit var nigateListenerService: NigateListenerService

    private val logger = Debugger(this::class.java)

    override fun typeSupport(): RequestTypeEnum {
        return RequestTypeEnum.RECOVERY_COMPLETE
    }

    override fun handleRequest(fromServer: String, msg: ByteBuffer, channel: Channel) {
        val recoveryComplete = RecoveryComplete(msg)
        val clusterGAO = recoveryComplete.getCommited()
        val localGAO = byteBufPreLogService.getCommitGAO()
        if (localGAO > clusterGAO) {
            logger.debug("当前世代集群日志最高为 $clusterGAO ，比本地 $localGAO 小，故需删除大于集群日志的所有日志")
            logService.discardAfter(clusterGAO)
        }

        /*
         * 当集群同步完毕，通知 RECOVERY_COMPLETE
         */
        nigateListenerService.onEvent(Event.RECOVERY_COMPLETE)
        logger.info("集群已经恢复正常，RECOVERY 完成")
    }
}