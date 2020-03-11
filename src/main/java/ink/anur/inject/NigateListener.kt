package ink.anur.inject

/**
 * Created by Anur IjuoKaruKas on 2020/3/8
 */
annotation class NigateListener(val onEvent: Event)

enum class Event {
    REGISTER_TO_SERVER,

    /**
     * 当集群可用时
     */
    CLUSTER_VALID,

    /**
     * 当集群不可用时
     */
    CLUSTER_INVALID,
}