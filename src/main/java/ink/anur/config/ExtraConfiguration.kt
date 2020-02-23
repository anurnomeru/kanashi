package ink.anur.config

import ink.anur.config.common.ConfigHelper
import ink.anur.config.common.ConfigurationEnum

/**
 * Created by Anur IjuoKaruKas on 2019/7/14
 */
object ExtraConfiguration : ConfigHelper() {

    fun isDebug(): Boolean {
        return CoordinateConfiguration.getConfig(ConfigurationEnum.DEBUG_MODE) { "enable" == it } as Boolean
    }

    fun neverReElectAfterHasLeader(): Boolean {
        return CoordinateConfiguration.getConfig(ConfigurationEnum.REELECT) { "true" == it } as Boolean
    }
}