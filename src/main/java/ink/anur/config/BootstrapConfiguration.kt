package ink.anur.config

/**
 * Created by Anur IjuoKaruKas on 2020/2/24
 */
object BootstrapConfiguration {

    private val ARGS = mutableMapOf<String, String>()

    val SERVER_NAME = "server_name"

    fun init(args: Array<String>) {
        for (arg in args) {
            val split = arg.split(":")
            ARGS[split[0]] = split[1]
        }
    }

    fun get(key: String): String? = ARGS[key]
}
