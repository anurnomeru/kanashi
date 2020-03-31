package ink.anur.log.common

import ink.anur.pojo.log.common.GenerationAndOffset
import ink.anur.pojo.log.base.LogItem

/**
 * Created by Anur IjuoKaruKas on 2019/10/11
 *
 * 此数据类型用于向引擎提交数据
 *
 * msgTime 用于返回 response
 *
 * 没有gao 代表这是一条查询指令 没有必要保存
 */
class EngineProcessEntry(val logItem: LogItem, val GAO: GenerationAndOffset? = null)