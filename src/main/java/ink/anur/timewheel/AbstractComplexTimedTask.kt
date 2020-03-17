package ink.anur.timewheel

import ink.anur.debug.Debugger

/**
 * Created by Anur IjuoKaruKas on 2020/3/18
 *
 * 抽象的定时任务，支持任务暂停，任务重启等功能
 */
abstract class AbstractComplexTimedTask {

    private var logger = Debugger(this.javaClass)

    /**
     * 此字段用作版本控制，定时任务仅执行小于等于自己版本的任务
     */
    @Volatile
    private var cvc: Long = 0

    /**
     * 内置的一个任务
     */
    @Volatile
    private var taskInner: TimedTask? = null

    protected fun isCancel(): Boolean = taskInner?.isCancel ?: true

    protected fun cancelTask() {
        cvc++
        taskInner?.cancel()
    }

    /**
     * 重构从某节点获取操作日志的定时任务
     *
     * 实际上就是不断创建定时任务，扔进时间轮，任务的内容则是调用 sendFetchPreLog 方法
     */
    protected fun rebuildTask(myVersion: Long, doSomeThing: Runnable) {
        if (cvc > myVersion) {
            logger.debug("Task is out of version")
            return
        }

        taskInner = TimedTask(internal(), doSomeThing)
        Timer.getInstance()
            .addTask(taskInner)
    }

    abstract fun internal(): Long
}