package ink.anur.common

/**
 * Created by Anur IjuoKaruKas on 2020/2/22
 */
abstract class KanashiRunnable : Runnable {

    fun start() {
        KanashiExecutors.execute(this);
    }
}