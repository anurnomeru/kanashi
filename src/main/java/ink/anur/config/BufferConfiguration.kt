package ink.anur.config

import ink.anur.config.common.ConfigHelper
import ink.anur.config.common.ConfigurationEnum
import ink.anur.inject.NigateBean

@NigateBean
class BufferConfiguration : ConfigHelper() {
    fun getMaxBufferPoolSize(): Long = getConfig(ConfigurationEnum.BUFFER_MAX_SIZE) { it.toLong() } as Long
}