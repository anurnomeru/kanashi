package ink.anur.timewheel

import ink.anur.common.KanashiRunnable

/**
 * Created by Anur IjuoKaruKas on 2020/3/18
 *
 * 抽象的定时任务，支持任务暂停，任务重启等功能
 */
abstract class AbstractComplexTimedTask {

    /**
     * 此字段用作版本控制，定时任务仅执行小于等于自己版本的任务
     */
    @Volatile
    private var cvc: Long = 0

    /**
     * 内置的一个任务
     */
    @Volatile
    private var taskInner: RebuildTask? = null

    /**
     * 直接取消这个定时任务
     */
    protected fun cancelTask() {
        cvc++
    }

    /**
     * 取消上次的任务，重新开始新一轮任务
     */
    protected fun rebuildTask(doSomeThing: () -> Unit) {
        cancelTask()
        RebuildTask(doSomeThing).start()
    }

    abstract fun internal(): Long


    inner class RebuildTask(val doSomething: () -> Unit) : KanashiRunnable() {

        override fun run() {
            val nowVersion = cvc
            while (nowVersion == cvc) {
                doSomething.invoke()
                Thread.sleep(internal())
            }
        }
    }
}