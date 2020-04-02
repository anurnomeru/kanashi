package ink.anur.pojo.log.common


/**
 * Created by Anur IjuoKaruKas on 2019/9/18
 */
object CommonApiTypeEnum {
    const val START_TRX: Byte = -128
    const val COMMIT_TRX: Byte = -127
    /**
     * 客户端主动回滚
     */
    const val ROLL_BACK: Byte = -126

    /**
     * 由于引擎以外的原因报错了，回滚
     */
    const val FORCE_ROLL_BACK: Byte = -125

    /**
     * 获取集群
     */
    const val GET_CLUSTER: Byte = -124
}