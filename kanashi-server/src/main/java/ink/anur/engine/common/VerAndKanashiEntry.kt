package ink.anur.engine.common

import ink.anur.pojo.log.ByteBufferKanashiEntry


/**
 * Created by Anur IjuoKaruKas on 2019/10/12
 */
class VerAndKanashiEntry(val trxId: Long, val kanashiEntry: ByteBufferKanashiEntry, var currentVersion: VerAndKanashiEntry? = null)