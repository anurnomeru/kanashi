package ink.anur.config

import ink.anur.common.struct.KanashiNode
import ink.anur.config.common.ConfigHelper
import ink.anur.config.common.ConfigurationEnum
import ink.anur.exception.ApplicationConfigException
import ink.anur.inject.NigateBean
import ink.anur.inject.NigatePostConstruct
import ink.anur.io.common.channel.ChannelHolder
import org.slf4j.LoggerFactory

/**
 * Created by Anur IjuoKaruKas on 2019/7/5
 *
 * 网络相关配置，都可以从这里获取
 */
@NigateBean
class InetSocketAddressConfiguration : ConfigHelper(), InetConfig {

    private lateinit var me: KanashiNode

    private val logger = LoggerFactory.getLogger(this::class.java)

    @NigatePostConstruct
    private fun init() {
        val nameFromConfig: String = getConfig(ConfigurationEnum.SERVER_NAME) { it } as String
        val name = BootstrapConfiguration.get(BootstrapConfiguration.SERVER_NAME) ?: nameFromConfig
        if (name == ChannelHolder.COORDINATE_LEADE_SIGN) {
            throw ApplicationConfigException(" 'LEADER' 为关键词，节点不能命名为这个关键词")
        }
        me = getNode(name)

        if (me == KanashiNode.NOT_EXIST) {
            throw ApplicationConfigException("服务名未正确配置，或者该服务不存在于服务配置列表中")
        }
        logger.info("current node is $me")
    }

    override fun getLocalServerName(): String {
        return me.serverName
    }

    override fun getNode(serverName: String?): KanashiNode {
        return getCluster().associateBy { kanashiLegal: KanashiNode -> kanashiLegal.serverName }[serverName] ?: KanashiNode.NOT_EXIST
    }

    fun getLocalCoordinatePort(): Int {
        return me.coordinatePort
    }

    fun getCluster(): List<KanashiNode> {
        return getConfigSimilar(ConfigurationEnum.CLIENT_ADDR) { pair ->
            val serverName = pair.key
            val split = pair.value
                .split(":".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
            KanashiNode(serverName, split[0], Integer.valueOf(split[1]))
        } as List<KanashiNode>
    }
}
