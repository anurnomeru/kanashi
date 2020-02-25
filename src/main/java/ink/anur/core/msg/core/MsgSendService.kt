package ink.anur.core.msg.core

import ink.anur.common.struct.common.AbstractStruct
import ink.anur.config.InetSocketAddressConfiguration
import ink.anur.exception.NetWorkException
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
class MsgSendService {

    private val logger = LoggerFactory.getLogger(this::class.java)

    private val LOCKER_MAPPING = HashMap<String, ReentrantLock>()

    @NigateInject
    private lateinit var inetSocketAddressConfiguration: InetSocketAddressConfiguration

    @NigateInject
    private lateinit var channelService: ChannelService

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
    fun doSend(serverName: String, body: AbstractStruct) {
        if (inetSocketAddressConfiguration.getLocalServerName() == serverName) {
            return
        }

        // 避免同个 channel 发生多线程问题

        val lock = getLock(serverName)
        lock.lock()

        try {
            val channel = channelService.getChannelHolder(ChannelService.ChannelType.COORDINATE).getChannel(serverName)
            logger.trace("正向节点发送 [$serverName] 关于 ${body.getOperationTypeEnum()} 的 request，大小为 ${body.totalSize()} bytes。")

            if (channel == null) {
                throw NetWorkException("与节点 [$serverName] 的连接已经断开，无法发送！")
            }
            channel.write(Unpooled.copyInt(body.totalSize()))
            body.writeIntoChannel(channel)
            channel.flush()
        } catch (t: Throwable) {
            logger.error("向节点发送 [$serverName] 关于 ${body.getOperationTypeEnum()} 的 request 失败： ${t.message}")
            throw t
        }
    }
}