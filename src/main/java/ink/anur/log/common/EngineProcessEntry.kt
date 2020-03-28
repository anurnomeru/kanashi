package ink.anur.log.common

import ink.anur.pojo.log.common.GenerationAndOffset
import ink.anur.pojo.log.base.LogItem

/**
 * Created by Anur IjuoKaruKas on 2019/10/11
 *
 * 此数据类型用于向引擎提交数据
 *
 * msgTime 用于返回 response
 */
class EngineProcessEntry(val logItem: LogItem, val GAO: GenerationAndOffset, val fromServer: String? = null, val msgTime: Long? = null)