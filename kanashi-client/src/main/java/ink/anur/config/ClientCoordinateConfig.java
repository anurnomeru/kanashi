package ink.anur.config;

import ink.anur.core.SpringContextHolder;
import ink.anur.inject.NigateBean;

/**
 * Created by Anur IjuoKaruKas on 2020/3/6
 */
@NigateBean
public class ClientCoordinateConfig implements CoordinateConfig {

    @Override
    public long getReSendBackOfMs() {
        return SpringContextHolder.getBean(KanashiConfig.class)
                                  .getReSendBackOffMs();
    }
}
