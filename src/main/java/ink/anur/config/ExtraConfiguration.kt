package ink.anur.config

import ink.anur.config.common.ConfigHelper
import ink.anur.config.common.ConfigurationEnum
import ink.anur.inject.NigateBean

/**
 * Created by Anur IjuoKaruKas on 2019/7/14
 */
@NigateBean
class ExtraConfiguration : ConfigHelper() {

    fun isDebug(): Boolean = getConfig(ConfigurationEnum.DEBUG_MODE) { "enable" == it } as Boolean

    fun neverReElectAfterHasLeader(): Boolean = getConfig(ConfigurationEnum.REELECT) { "true" == it } as Boolean
}