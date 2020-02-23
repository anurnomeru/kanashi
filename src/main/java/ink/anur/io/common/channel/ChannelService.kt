package ink.anur.io.common.channel

import ink.anur.exception.NoSuchChannelException
import ink.anur.inject.NigateBean
import java.util.concurrent.ConcurrentHashMap

/**
 * Created by Anur IjuoKaruKas on 2020/2/23
 *
 * 管理所有连接的Channel服务
 */
@NigateBean
class ChannelService {

    private val HOLDER_MAPPING = ConcurrentHashMap<ChannelType, ChannelHolder>()

    /**
     * 获取 协调管道 或者 业务管道
     */
    fun getChannelHolder(channelType: ChannelType): ChannelHolder {
        var channelManager: ChannelHolder? = HOLDER_MAPPING[channelType]
        if (channelManager == null) {
            synchronized(this::class.java) {
                channelManager = HOLDER_MAPPING[channelType]
                if (channelManager == null) {

                    channelManager = ChannelHolder()
                    HOLDER_MAPPING[channelType] = channelManager!!
                }
            }
        }

        return channelManager ?: throw NoSuchChannelException("channel type $channelType not enable")
    }

    /**
     * 连接类型
     */
    enum class ChannelType {
        COORDINATE,
        SERVICE
    }
}