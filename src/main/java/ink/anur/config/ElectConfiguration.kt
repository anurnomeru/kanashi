package ink.anur.config

import ink.anur.config.common.ConfigHelper
import ink.anur.config.common.ConfigurationEnum
import ink.anur.inject.NigateBean

/**
 * Created by Anur IjuoKaruKas on 2019/7/5
 */
@NigateBean
class ElectConfiguration : ConfigHelper() {

    fun getElectionTimeoutMs(): Int = getConfig(ConfigurationEnum.ELECT_ELECTION_TIMEOUT_MS) { Integer.valueOf(it) } as Int

    fun getVotesBackOffMs(): Int = getConfig(ConfigurationEnum.ELECT_VOTES_BACK_OFF_MS) { Integer.valueOf(it) } as Int

    fun getHeartBeatMs(): Int = getConfig(ConfigurationEnum.ELECT_HEART_BEAT_MS) { Integer.valueOf(it) } as Int
}