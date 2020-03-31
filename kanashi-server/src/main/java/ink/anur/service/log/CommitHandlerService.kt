package ink.anur.service.log

import ink.anur.core.common.AbstractRequestMapping
import ink.anur.core.request.RequestProcessCentreService
import ink.anur.debug.Debugger
import ink.anur.engine.prelog.ByteBufPreLogService
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.pojo.enumerate.RequestTypeEnum
import ink.anur.pojo.log.Commit
import ink.anur.pojo.log.CommitResponse
import io.netty.channel.Channel
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2020/3/17
 *
 *  主 -> 从
 *
 * 子节点处理来自 leader 的 commit 请求，并 commit 自己的 preLog
 */
@NigateBean
class CommitHandlerService : AbstractRequestMapping() {

    @NigateInject
    private lateinit var byteBufPreLogService: ByteBufPreLogService

    @NigateInject
    private lateinit var requestProcessCentreService: RequestProcessCentreService

    override fun typeSupport(): RequestTypeEnum {
        return RequestTypeEnum.COMMIT
    }

    override fun handleRequest(fromServer: String, msg: ByteBuffer, channel: Channel) {
        val commit = Commit(msg)
        byteBufPreLogService.commit(commit.canCommitGAO)

        val commitGAO = byteBufPreLogService.getCommitGAO()
        requestProcessCentreService.send(fromServer, CommitResponse(commitGAO))
    }
}