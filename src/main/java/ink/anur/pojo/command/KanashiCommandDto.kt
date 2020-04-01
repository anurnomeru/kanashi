package ink.anur.pojo.command

import ink.anur.pojo.enumerate.RequestTypeEnum
import ink.anur.pojo.log.KanashiCommand
import ink.anur.pojo.log.base.LogItem
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2020/3/28
 */
class KanashiCommandDto : LogItem {

    constructor(byteBuffer: ByteBuffer) : super(byteBuffer)

    constructor(key: String, value: KanashiCommand) : super(key, value)
}