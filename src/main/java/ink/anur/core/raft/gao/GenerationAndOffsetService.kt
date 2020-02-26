package ink.anur.core.raft.gao

import ink.anur.inject.NigateBean
import org.slf4j.LoggerFactory

/**
 * Created by Anur IjuoKaruKas on 2020/2/26
 */
@NigateBean
class GenerationAndOffsetService {

    private val logger = LoggerFactory.getLogger(this::class.java)

    fun getInitialGao(): GenerationAndOffset {
        return GenerationAndOffset(1, 1)
    }
}