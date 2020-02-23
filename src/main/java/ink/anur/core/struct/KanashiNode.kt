package ink.anur.core.struct


class KanashiNode(val serverName: String, val host: String, val electionPort: Int, val coordinatePort: Int) {

    companion object {
        val NOT_EXIST = KanashiNode("", "", 0, 0)
    }

    /**
     * 是否是本地节点
     */
    fun isLocalNode(): Boolean {
        return this.serverName == InetSocketAddressConfiguration.getServerName()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as KanashiNode

        if (serverName != other.serverName) return false
        if (host != other.host) return false
        if (electionPort != other.electionPort) return false
        if (coordinatePort != other.coordinatePort) return false
        return true
    }

    override fun hashCode(): Int {
        var result = serverName.hashCode()
        result = 31 * result + host.hashCode()
        result = 31 * result + electionPort
        result = 31 * result + coordinatePort
        return result
    }
}