package ink.anur.pojo.enumerate

import ink.anur.pojo.EmptyStruct
import ink.anur.pojo.Register
import ink.anur.pojo.RegisterResponse
import ink.anur.pojo.coordinate.Voting
import ink.anur.pojo.common.AbstractStruct
import ink.anur.exception.KanashiException
import ink.anur.pojo.coordinate.Canvass
import ink.anur.pojo.HeartBeat
import ink.anur.pojo.log.Commit
import ink.anur.pojo.log.CommitResponse
import ink.anur.pojo.log.RecoveryComplete
import ink.anur.pojo.log.RecoveryReporter
import ink.anur.pojo.command.KanashiCommandBatchDto
import ink.anur.pojo.command.Fetch
import ink.anur.pojo.command.KanashiCommandResponse
import java.util.HashMap

/**
 * Created by Anur IjuoKaruKas on 2020/2/22
 */
enum class RequestTypeEnum(val byteSign: Int, val clazz: Class<out AbstractStruct>) {

    /**
     * 无类型
     */
    EMPTY_STRUCT(-1, EmptyStruct::class.java),

    /**
     * 心跳
     */
    HEAT_BEAT(9999, HeartBeat::class.java),

    /**
     * 协调从节点向主节点注册
     */
    REGISTER(10000, Register::class.java),

    /**
     * 协调从节点向主节点注册 的回复
     */
    REGISTER_RESPONSE(10001, RegisterResponse::class.java),

    /**
     * 进行拉票
     */
    CANVASS(10002, Canvass::class.java),

    /**
     * 进行投票
     */
    VOTING(10003, Voting::class.java),

    /**
     * 请求 fetch log
     */
    FETCH(20000, Fetch::class.java),

    /**
     * fetch 结果
     */
    FETCH_RESPONSE(20001, KanashiCommandBatchDto::class.java),

    /**
     * 上报recovery进度
     */
    RECOVERY_REPORTER(20002, RecoveryReporter::class.java),

    /**
     * 表示已经recovery完毕
     */
    RECOVERY_COMPLETE(20003, RecoveryComplete::class.java),

    /**
     * 告知 follower 已经可以commit了
     * <p>
     * 集群成员收到 COMMIT 消息时,需要回复一个 COMMIT RESPONSE,表明自己的 commit 进度, leader
     */
    COMMIT(20004, Commit::class.java),

    /**
     * 当收到leader发来的可提交进度时,进行进度提交,并且进行当前最大提交进度的回包
     */
    COMMIT_RESPONSE(20005, CommitResponse::class.java),

    /**
     * 从客户端发来的指令
     */
    COMMAND(99999, KanashiCommandBatchDto::class.java),

    /**
     * 成功回复
     */
    COMMAND_RESPONSE(99998, KanashiCommandResponse::class.java),
    ;

    companion object {
        private val byteSignMap = HashMap<Int, RequestTypeEnum>()

        init {
            val unique = mutableSetOf<Int>()
            for (value in values()) {
                if (!unique.add(value.byteSign)) {
                    throw KanashiException("RequestTypeEnum 中，byteSign 不可重复。");
                }
                byteSignMap[value.byteSign] = value;
            }
        }

        fun parseByByteSign(byteSign: Int): RequestTypeEnum = byteSignMap[byteSign] ?: throw UnsupportedOperationException()
    }
}