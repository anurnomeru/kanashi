package ink.anur.config

import ink.anur.config.common.ConfigHelper
import ink.anur.config.common.ConfigurationEnum
import ink.anur.inject.NigateBean

/**
 * Created by Anur IjuoKaruKas on 2019/7/5
 */
@NigateBean
class CoordinateConfiguration : ConfigHelper() {

    fun getFetchBackOfMs(): Int = getConfig(ConfigurationEnum.COORDINATE_FETCH_BACK_OFF_MS) { Integer.valueOf(it) } as Int
}
