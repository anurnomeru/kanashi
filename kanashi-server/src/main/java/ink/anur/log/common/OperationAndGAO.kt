package ink.anur.log.common

import ink.anur.core.raft.gao.GenerationAndOffset
import ink.anur.pojo.log.Operation

/**
 * Created by Anur IjuoKaruKas on 2019/10/11
 */
class OperationAndGAO(val operation: Operation, val GAO: GenerationAndOffset)