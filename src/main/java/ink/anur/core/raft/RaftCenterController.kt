package ink.anur.core.raft

import ink.anur.common.KanashiRunnable
import ink.anur.common.struct.Voting
import ink.anur.config.ElectConfiguration
import ink.anur.config.InetSocketAddressConfiguration
import ink.anur.core.raft.gao.GenerationAndOffset
import ink.anur.core.raft.gao.GenerationAndOffsetService
import ink.anur.core.rentrant.ReentrantLocker
import ink.anur.inject.NigateBean
import ink.anur.inject.NigateInject
import ink.anur.timewheel.TimedTask
import ink.anur.timewheel.Timer
import ink.anur.util.TimeUtil
import org.slf4j.LoggerFactory
import java.util.Optional
import java.util.Random
import java.util.concurrent.ConcurrentHashMap
import javax.annotation.PostConstruct

/**
 * Created by Anur IjuoKaruKas on 2020/2/26
 *
 * 选举相关、raft核心控制代码
 */
@NigateBean
class RaftCenterController : KanashiRunnable() {

    @NigateInject
    private lateinit var electConfiguration: ElectConfiguration

    @NigateInject
    private lateinit var inetSocketAddressConfiguration: InetSocketAddressConfiguration

    @NigateInject
    private lateinit var generationAndOffsetService: GenerationAndOffsetService

    @NigateInject
    private lateinit var electionMetaService: ElectionMetaService

    private var ELECTION_TIMEOUT_MS: Int = -1

    private var VOTES_BACK_OFF_MS: Int = -1

    private var HEART_BEAT_MS: Int = -1

    private val RANDOM = Random()

    private val reentrantLocker = ReentrantLocker()

    private val logger = LoggerFactory.getLogger(this::class.java)

    /**
     * 所有正在跑的定时任务
     */
    private var taskMap = ConcurrentHashMap<TaskEnum, TimedTask>()

    @PostConstruct
    private fun init() {
        logger.info("初始化选举控制器 ElectOperator，本节点为 {}", inetSocketAddressConfiguration.getLocalServerName())
        this.name = "RaftCenterController"
        this.start()

        ELECTION_TIMEOUT_MS = electConfiguration.getElectionTimeoutMs()
        VOTES_BACK_OFF_MS = electConfiguration.getVotesBackOffMs()
        HEART_BEAT_MS = electConfiguration.getHeartBeatMs()
    }

    override fun run() {
        logger.debug("初始化选举控制器 启动中")



        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    /**
     * 成为候选者的任务，（重复调用则会取消之前的任务，收到来自leader的心跳包，就可以重置一下这个任务）
     *
     * 没加锁，因为这个任务需要频繁被调用，只要收到leader来的消息就可以调用一下
     */
    private fun becomeCandidateAndBeginElectTask(generation: Long) {
        reentrantLocker.lockSupplier {
            this.cancelCandidateAndBeginElectTask("正在重置发起下一轮选举的退避时间")

            // The election timeout is randomized to be between 150ms and 300ms.
            val electionTimeout = ELECTION_TIMEOUT_MS + (ELECTION_TIMEOUT_MS * RANDOM.nextFloat()).toLong()
            val timedTask = TimedTask(electionTimeout) { this.beginElect(generation) }
            Timer.getInstance()
                .addTask(timedTask)

            taskMap[TaskEnum.BECOME_CANDIDATE] = timedTask
        }
    }

    /**
     * 取消成为候选者的任务，成为leader，或者调用 [.becomeCandidateAndBeginElectTask] （心跳）
     * 时调用，也就是说如果没能成为leader，又会重新进行一次选主，直到成为leader，或者follower。
     */
    private fun cancelCandidateAndBeginElectTask(msg: String) {
        reentrantLocker.lockSupplier {
            logger.trace(msg)
            taskMap[TaskEnum.BECOME_CANDIDATE]?.cancel()
        }
    }

    /**
     * 强制更新世代信息
     */
    private fun updateGeneration(reason: String) {
        reentrantLocker.lockSupplier {
            logger.debug("强制更新当前世代 {} -> {}", electionMetaService.generation, electionMetaService.generation + 1)

            if (!this.init(meta.getGeneration() + 1, reason)) {
                updateGeneration(reason)
            }
            null
        }
    }

    /**
     * 成为候选者
     */
    private fun becomeCandidate() {
        reentrantLocker.lockSupplier {
            electionMetaService.becomeCandidate()
        }
    }

    /**
     * 当选票大于一半以上时调用这个方法，如何去成为一个leader
     */
    private fun becomeLeader(): Boolean {
        return reentrantLocker.lockSupplierCompel {
            electionMetaService.becomeLeader()
            this.cancelAllTask()
            this.initHeartBeatTask()
            true
        }
    }

    /**
     * 开始进行选举
     *
     * 1、首先更新一下世代信息，重置投票箱和投票记录
     * 2、成为候选者
     * 3、给自己投一票
     * 4、请求其他节点，要求其他节点给自己投票
     */
    private fun beginElect(generation: Long) {
        reentrantLocker.lockSupplier {

            if (electionMetaService.generation != generation) {// 存在这么一种情况，虽然取消了选举任务，但是选举任务还是被执行了，所以这里要多做一重处理，避免上个周期的任务被执行
                return@lockSupplier
            }

            if (electionMetaService.beginElectTime == 0L) {// 用于计算耗时
                electionMetaService.beginElectTime = TimeUtil.getTime()
            }

            logger.info("Election Timeout 到期，可能期间内未收到来自 Leader 的心跳包或上一轮选举没有在期间内选出 Leader，故本节点即将发起选举")
            updateGeneration("本节点发起了选举")// meta.getGeneration() ++

            // 成为候选者
            logger.info("本节点正式开始世代 {} 的选举", electionMetaService.generation)
            this.becomeCandidate()
            val votes = Voting(true, false, electionMetaService.generation, electionMetaService.generation)

            // 给自己投票箱投票
            this.receiveVotesResponse(votes)

            // 记录一下，自己给自己投了票
            meta.setVoteRecord(votes)

            // 让其他节点给自己投一票
            this.askForVoteTask(Votes(meta.getGeneration(), InetSocketAddressConfiguration.INSTANCE.getServerName()), 0)
        }
    }

    /**
     * 给当前节点的投票箱投票
     */
    fun receiveVotesResponse(serverName: String, voting: Voting) {
        reentrantLocker.lockSupplier {

            // 已经有过回包了，无需再处理
            if (electionMetaService.box[serverName] != null) {
                return@lockSupplier
            }
            val voteSelf = serverName == inetSocketAddressConfiguration.getLocalServerName()
            if (voteSelf) {
                logger.info("本节点在世代 {} 转变为候选者，给自己先投一票", electionMetaService.generation)
            } else {
                logger.info("收到来自节点 {} 的投票应答，其世代为 {}", serverName, voting.generation)
            }

            if (voting.fromLeaderNode) {
                logger.info("来自节点 {} 的投票应答表明其身份为 Leader，本轮拉票结束。", serverName)
                this.receiveHeatBeat(votesResponse.getServerName(), votesResponse.getGeneration(),
                    String.format("收到来自 Leader 节点的投票应答，自动将其视为来自 Leader %s 世代 %s 节点的心跳包", meta.getHeartBeat()
                        .getServerName(), votesResponse.getGeneration()))
            }

            if (electionMetaService.generation > voting.askVoteGeneration) {// 如果选票的世代小于当前世代，投票无效
                logger.info("来自节点 {} 的投票应答世代是以前世代 {} 的选票，选票无效", serverName, voting.askVoteGeneration)
                return@lockSupplier
            }

            // 记录一下投票结果
            electionMetaService.box[serverName] = voting.agreed

            if (voting.agreed) {
                if (!voteSelf) {
                    logger.info("来自节点 {} 的投票应答有效，投票箱 + 1", serverName)
                }

                val hanabiNodeList = electionMetaService.clusters!!
                val voteCount = electionMetaService.box.values.filter { it }.count()

                logger.info("集群中共 {} 个节点，本节点当前投票箱进度 {}/{}", hanabiNodeList.size, voteCount, electionMetaService.quorum)

                // 如果获得的选票已经大于了集群数量的一半以上，则成为leader
                if (voteCount == electionMetaService.quorum) {
                    logger.info("选票过半，本节点 {] 即将上位成为 leader 节点", inetSocketAddressConfiguration.getLocalServerName())
                    this.becomeLeader()
                }
            } else {
                logger.info("节点 {} 在世代 {} 的投票应答为：拒绝给本节点在世代 {} 的选举投票（当前世代 {}）", serverName, voting.generation, voting.askVoteGeneration,
                    electionMetaService.generation)
            }
        }
    }
}