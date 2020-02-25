package ink.anur.core.msg.common

import ink.anur.common.KanashiExecutors
import ink.anur.core.rentrant.ReentrantReadWriteLocker
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2020/2/24
 *
 * 对于一个请求，总有发送回调，消息收到回调等等配置，就是通过这个来完成的
 */
abstract class RequestExtProcessor : ReentrantReadWriteLocker() {

    /**
     * 是否已经完成了这个请求过程（包括接收response）
     */
    @Volatile
    private var complete = false

    /**
     * 在收到回复后怎么处理
     */
    abstract fun howToConsumeResponse(response: ByteBuffer)

    /**
     * 在收到回复后并且处理完成后怎么处理
     */
    abstract fun afterCompleteReceive()

    /**
     * 是否已经处理完成
     */
    fun isComplete(): Boolean = complete

    /**
     * 已经完成了此任务
     */
    fun complete() = writeLockSupplier {
        if (!complete) {
            complete = true
            KanashiExecutors.execute(Runnable { afterCompleteReceive() })
        }
    }

    /**
     * 已经完成了此任务
     */
    fun complete(byteBuffer: ByteBuffer) = writeLockSupplier {
        if (!complete) {
            complete = true

            KanashiExecutors.execute(Runnable {
                howToConsumeResponse(byteBuffer)
                afterCompleteReceive()
            })
        }
    }
}