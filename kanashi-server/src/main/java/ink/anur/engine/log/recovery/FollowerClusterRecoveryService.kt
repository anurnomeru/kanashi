package ink.anur.engine.log.recovery

import ink.anur.core.raft.ElectionMetaService
import ink.anur.debug.Debugger
import ink.anur.inject.Event
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateListener

/**
 * Created by Anur IjuoKaruKas on 4/9/2019
 *
 * 当服务器挂机或者不正常时，需要进行集群日志的恢复
 *
 * 当选主成功后
 *
 * - 所有节点进行coordinate的注册，注册时上报其最大 commit offset
 *
 * - 进行 recovery waiting n sec，直到所有节点上报数据
 *
 * -- 是否达到半数节点上报 => no => 节点一直阻塞，直到有半数节点上报
 *
 * |
 * |
 * V
 * yes
 *
 * - 获取最大的commit，作为 recovery point，最小的 commit 则作为 commit GAO
 *
 * -- leader 是否达到此 commit 数据 => no => 向拥有此数据的节点进行 fetch
 *
 * -- 下发指令，删除大于此 recovery point 的数据（针对前leader）
 *
 * |
 * |
 * V
 *
 * 集群可用
 *
 * //////////////////////////////////////////////////////////////////////////////////
 *
 * 集群可用后连上leader的需要做特殊处理：
 *
 * 需要检查当前世代 的last Offset，进行check，如果与leader不符，则需要truncate后恢复可用。
 */
@NigateBean
class FollowerClusterRecoveryService {

    private val logger = Debugger(this.javaClass)

    private lateinit var electionMetaService: ElectionMetaService


//    /**
//     * 当子节点检测不到 Leader 的心跳后，断开协调控制器
//     */
//    @NigateListener(onEvent = Event.CLUSTER_INVALID)
//    private fun whileClusterInvalid() {
////        if (!electionMetaService.isLeader()) {// todo
////            CoordinateClientOperator.getInstance(InetSocketAddressConfiguration.getNode(ElectMeta.leader)).shutDown()
////        }
//    }
//
//    private fun whileClusterValid() {
//
//    }
}