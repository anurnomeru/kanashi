package ink.anur.core.struct

import ink.anur.config.InetSocketAddressConfiguration
import ink.anur.inject.Nigate


class KanashiNode(val serverName: String, val host: String, val servicePort: Int, val coordinatePort: Int) {

    companion object {
        val NOT_EXIST = KanashiNode("", "", 0, 0)
    }

    /**
     * 是否是本地节点
     */
    fun isLocalNode(): Boolean {
        return this.serverName == Nigate.getBeanByClass(InetSocketAddressConfiguration::class.java).getLocalServerName()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as KanashiNode

        if (serverName != other.serverName) return false
        if (host != other.host) return false
        if (servicePort != other.servicePort) return false
        if (coordinatePort != other.coordinatePort) return false
        return true
    }

    override fun hashCode(): Int {
        var result = serverName.hashCode()
        result = 31 * result + host.hashCode()
        result = 31 * result + servicePort
        result = 31 * result + coordinatePort
        return result
    }

    override fun toString(): String {
        return "KanashiNode(serverName='$serverName', host='$host', servicePort=$servicePort, coordinatePort=$coordinatePort)"
    }

}