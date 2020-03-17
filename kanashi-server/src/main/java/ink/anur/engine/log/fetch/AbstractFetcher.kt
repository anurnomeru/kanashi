//package ink.anur.engine.log.fetch
//
//import ink.anur.config.CoordinateConfiguration
//import ink.anur.core.common.RequestExtProcessor
//import ink.anur.core.raft.ElectionMetaService
//import ink.anur.core.request.RequestProcessCentreService
//import ink.anur.engine.log.prelog.ByteBufPreLogService
//import ink.anur.inject.NigateInject
//import ink.anur.mutex.ReentrantReadWriteLocker
//import ink.anur.pojo.server.FetchResponse
//import ink.anur.pojo.server.Fetch
//import ink.anur.timewheel.TimedTask
//import ink.anur.timewheel.Timer
//import org.slf4j.LoggerFactory
//import java.util.concurrent.locks.ReentrantLock
//
///**
// * Created by Anur IjuoKaruKas on 2020/3/11
// *
// * 负责创建拉取日志的任务
// */
//abstract class AbstractFetcher : ReentrantReadWriteLocker() {
//
//    private var logger = LoggerFactory.getLogger(this::class.java)
//
//    @NigateInject
//    private lateinit var electionMetaService: ElectionMetaService
//
//    @NigateInject
//    private lateinit var coordinateConfiguration: CoordinateConfiguration
//
//    @NigateInject
//    private lateinit var requestProcessCentreService: RequestProcessCentreService
//
//    @NigateInject
//    private lateinit var byteBufPreLogService: ByteBufPreLogService
//
//    /**
//     * 此字段用作版本控制，定时任务仅执行小于等于自己版本的任务
//     *
//     * Coordinate Version Control
//     */
//    @Volatile
//    private var cvc: Long = 0
//
//    /**
//     * 作为 Follower 时有效，此任务不断从 Leader 节点获取 PreLog
//     */
//    private var fetchPreLogTask: TimedTask? = null
//
//    /**
//     * Fetch 锁
//     */
//    private var fetchLock = ReentrantLock()
//
//    protected fun fetchLocker(doSomething: () -> Unit) {
//        fetchLock.lock()
//        try {
//            doSomething.invoke()
//        } finally {
//            fetchLock.unlock()
//        }
//    }
//
//    protected fun cancelFetchTask() {
//        cvc++
//        fetchPreLogTask?.cancel()
//        logger.info("取消 FetchPreLog 定时任务")
//    }
//
//
//    /**
//     * 构建开始从 Leader 同步操作日志的定时任务
//     */
//    protected fun startToFetchFromLeader() {
//        if (!electionMetaService.isLeader()) {
//            writeLocker {
//                // 如果节点非Leader，需要连接 Leader，并创建 Fetch 定时任务
//                fetchLocker { rebuildFetchTask(cvc, electionMetaService.leader!!) }
//            }
//        }
//    }
//
//    /**
//     * 构建开始从 某节点 同步操作日志的定时任务
//     */
//    protected fun startToFetchFrom(serverName: String) {
//        writeLocker {
//            // 如果节点非Leader，需要连接 Leader，并创建 Fetch 定时任务
//            fetchLocker { rebuildFetchTask(cvc, serverName) }
//        }
//    }
//
//    /**
//     * 重构从某节点获取操作日志的定时任务
//     *
//     * 实际上就是不断创建定时任务，扔进时间轮，任务的内容则是调用 sendFetchPreLog 方法
//     */
//    private fun rebuildFetchTask(myVersion: Long, fetchFrom: String) {
//        if (cvc > myVersion) {
//            logger.debug("sendFetchPreLog Task is out of version.")
//            return
//        }
//
//        fetchPreLogTask = TimedTask(coordinateConfiguration.getReSendBackOfMs()) { sendFetchPreLog(myVersion, fetchFrom) }
//        Timer.getInstance()
//            .addTask(fetchPreLogTask)
//    }
//
//
//    /**
//     * 主要负责定时 Fetch 消息
//     *
//     * 新建一个 Fetcher 用于拉取消息，将其发送给 Leader，并在收到回调后，调用 CONSUME_FETCH_RESPONSE 消费回调，且重启拉取定时任务
//     */
//    private fun sendFetchPreLog(myVersion: Long, fetchFrom: String) {
//        fetchLock.lock()
//        try {
//            fetchPreLogTask?.takeIf { !it.isCancel }?.run {
//                requestProcessCentreService.send(fetchFrom,
//                    Fetch(byteBufPreLogService.getPreLogGAO()),
//                    RequestExtProcessor({
//                        readLocker {
//                            val fetchResponse = FetchResponse(it)
//                            if (fetchResponse.generation != FetchResponse.Invalid) {
//                                howToConsumeFetchResponse(fetchFrom, fetchResponse)
//                            }
//                            rebuildFetchTask(myVersion, fetchFrom)
//                        }
//                    }),
//                    false
//                )
//            }
//        } finally {
//            fetchLock.unlock()
//        }
//    }
//
//    /**
//     * 子类定义如何消费Response
//     */
//    abstract fun howToConsumeFetchResponse(fetchFrom: String, fetchResponse: FetchResponse)
//}