package ink.anur.core.central.core

import ink.anur.struct.common.AbstractStruct
import ink.anur.config.InetSocketAddressConfiguration
import ink.anur.core.client.CoordinateClientService
import ink.anur.core.struct.KanashiNode
import ink.anur.exception.NetWorkException
import ink.anur.exception.UnKnownNodeException
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.io.common.channel.ChannelService
import io.netty.buffer.Unpooled
import org.slf4j.LoggerFactory
import java.util.HashMap
import java.util.concurrent.locks.ReentrantLock

/**
 * Created by Anur IjuoKaruKas on 2020/2/25
 */
@NigateBean
class SenderService {

    @NigateInject
    private lateinit var inetSocketAddressConfiguration: InetSocketAddressConfiguration

    @NigateInject
    private lateinit var channelService: ChannelService

    @NigateInject
    private lateinit var coordinateClientService: CoordinateClientService

    private val logger = LoggerFactory.getLogger(this::class.java)

    private val LOCKER_MAPPING = HashMap<String, ReentrantLock>()

    /**
     * 在向同一个服务发送东西时需要加锁
     */
    @Synchronized
    private fun getLock(serverName: String): ReentrantLock {
        var lock: ReentrantLock? = LOCKER_MAPPING[serverName]
        if (lock == null) {
            lock = ReentrantLock()
            LOCKER_MAPPING[serverName] = lock
        }
        return lock
    }

    /**
     * 向某个服务发送东西~
     */
    fun doSend(serverName: String, body: AbstractStruct): Throwable? {
        if (inetSocketAddressConfiguration.getLocalServerName() == serverName) {
            return null
        }

        val lock = getLock(serverName)
        try {
            lock.lock()
            val channel = channelService.getChannelHolder(ChannelService.ChannelType.COORDINATE).getChannel(serverName)

            if (channel == null) {
                val node = inetSocketAddressConfiguration.getNode(serverName)
                if (node == KanashiNode.NOT_EXIST) {
                    return UnKnownNodeException("无法在配置文件中找到节点 $serverName，故无法主动连接该节点")
                }
                coordinateClientService.connect(node)
            }

            if (channel == null) {
                return NetWorkException("还未与节点 [$serverName] 建立连接，无法发送！")
            }
            channel.write(Unpooled.copyInt(body.totalSize()))
            body.writeIntoChannel(channel)
            channel.flush()
        } catch (t: Throwable) {
            logger.error("向节点发送 [$serverName] 关于 ${body.getOperationTypeEnum()} 的请求失败： ${t.message}")
            return t
        } finally {
            lock.unlock()
        }

        return null
    }
}