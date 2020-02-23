package ink.anur.config

import ink.anur.config.common.ConfigHelper
import ink.anur.config.common.ConfigurationEnum

object BufferConfiguration : ConfigHelper() {

    fun getMaxBufferPoolSize(): Long {
        return CoordinateConfiguration.getConfig(ConfigurationEnum.BUFFER_MAX_SIZE) { it.toLong() } as Long
    }
}