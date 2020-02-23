package ink.anur.io.common.channel

import ink.anur.core.rentrant.ReentrantReadWriteLocker
import ink.anur.exception.NoSuchChannelException
import io.netty.channel.Channel

/**
 * Created by Anur IjuoKaruKas on 2020/2/23
 *
 * 管理所有连接
 */
class ChannelHolder : ReentrantReadWriteLocker() {

    companion object {
        val COORDINATE_LEADE_SIGN = "LEADER"
    }

    /**
     * 记录了服务名和 channel 的映射
     */
    private val serverChannelMap: MutableMap<String/* serverName */, Channel> = mutableMapOf()

    /**
     * 记录了 channel 和服务名的映射
     */
    private val channelServerMap: MutableMap<Channel, String/* serverName */> = mutableMapOf()

    /**
     * 如果还没连接上服务，是会返回空的
     */
    fun getChannel(serverName: String): Channel = this.readLockSupplier {
        serverChannelMap[serverName]
            ?: throw NoSuchChannelException("No such channel name $serverName connected")
    }!!

    /**
     * 如果还没连接上服务，是会返回空的
     */
    fun getChannelName(channel: Channel): String = this.readLockSupplier {
        channelServerMap[channel]
            ?: throw NoSuchChannelException("No such channel $channel connected")
    }!!


    /**
     * 向 channelManager 注册服务
     */
    fun register(serverName: String, channel: Channel) {
        this.writeLockSupplier {
            serverChannelMap[serverName] = channel
            channelServerMap[channel] = serverName
        }
    }

    /**
     * 根据服务名来注销服务
     */
    fun unRegister(serverName: String) {
        this.writeLockSupplier {
            val channel = serverChannelMap.remove(serverName)
            channelServerMap.remove(channel)
        }
    }

    /**
     * 根据管道来注销服务
     */
    fun unRegister(channel: Channel) {
        this.writeLockSupplier {
            val serverName = channelServerMap.remove(channel)
            serverChannelMap.remove(serverName)
        }
    }
}