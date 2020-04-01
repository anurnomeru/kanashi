package ink.anur.config;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import ink.anur.common.Constant;
import ink.anur.common.struct.KanashiNode;
import ink.anur.core.SpringContextHolder;
import ink.anur.inject.NigateBean;

/**
 * Created by Anur IjuoKaruKas on 2020/3/7
 */
@NigateBean
public class ClientInetConfig implements InetConfig {

    @Autowired
    private KanashiConfig kanashiConfig;

    @NotNull
    @Override
    public String getLocalServerName() {
        return Constant.INSTANCE.getCLIENT();
    }

    @NotNull
    @Override
    public KanashiNode getNode(@Nullable String serverName) {
        KanashiConfig bean = SpringContextHolder.getBean(KanashiConfig.class);
        return new KanashiNode(Constant.INSTANCE.getSERVER(), bean.getHost(), bean.getPort());
    }
}
