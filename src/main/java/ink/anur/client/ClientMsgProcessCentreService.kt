package ink.anur.client

import ink.anur.common.struct.KanashiNode
import ink.anur.core.client.ClientOperateHandler
import ink.anur.core.request.MsgProcessCentreService
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import io.netty.channel.ConnectTimeoutException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Created by Anur IjuoKaruKas on 2020/4/6
 *
 * 专用于客户端与服务端发起连接，其不同之处是，存在“连接池”这个概念，且可与服务端发起多个TCP连接
 *
 * 在一个事务中，不可跨连接（其实是为了避免访问不同机器导致）。
 */
@NigateBean
class ClientMsgProcessCentreService {

    @NigateInject
    private lateinit var msgProcessCentreService: MsgProcessCentreService

    /**
     * 代表当前节点持有的集群信息，第零个代表是 leader
     */
    private var clusters = mutableListOf<KanashiNode>()

    private var connections = mutableListOf<ClientOperateHandler>()

    /**
     * 发起向某服务的连接，如果5秒内没有成功，则抛出异常
     */
    private fun connectTo(kanashiNode: KanashiNode) {

        val connectLatch = CountDownLatch(1)
        val clientOperateHandler = ClientOperateHandler(kanashiNode, { connectLatch.countDown() })
        clientOperateHandler.start()
        if (!connectLatch.await(5, TimeUnit.SECONDS)) {
            clientOperateHandler.shutDown()
            throw ConnectTimeoutException()
        } else {
            connections.add(clientOperateHandler)
        }
    }
}