package ink.anur.inject

/**
 * Created by Anur IjuoKaruKas on 2020/3/8
 */
annotation class NigateListener(val onEvent: Event)

enum class Event {
    REGISTER_TO_SERVER
}