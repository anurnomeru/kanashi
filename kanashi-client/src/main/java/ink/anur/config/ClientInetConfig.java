package ink.anur.config;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ink.anur.common.struct.KanashiNode;
import ink.anur.inject.NigateBean;

/**
 * Created by Anur IjuoKaruKas on 2020/3/7
 */
@NigateBean
public class ClientInetConfig implements InetConfig {

    @NotNull
    @Override
    public String getLocalServerName() {
        return "CLIENT";
    }

    @NotNull
    @Override
    public KanashiNode getNode(@Nullable String serverName) {
        return null;
    }
}
