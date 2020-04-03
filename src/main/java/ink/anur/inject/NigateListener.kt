package ink.anur.inject

/**
 * Created by Anur IjuoKaruKas on 2020/3/8
 */
annotation class NigateListener(val onEvent: Event)

enum class Event {

    /**
     * 连接上server后（client）
     */
    REGISTER_TO_SERVER,

    /**
     * 选举成功
     */
    CLUSTER_VALID,

    /**
     * 当节点没有 leader 时
     */
    CLUSTER_INVALID,

    /**
     * 当集群日志恢复完毕
     */
    RECOVERY_COMPLETE,

    /**
     * 日志加载完毕
     */
    LOG_LOAD_COMPLETE,
}