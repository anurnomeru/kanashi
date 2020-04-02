package ink.anur.config;

import java.util.HashMap;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ink.anur.common.Constant;
import ink.anur.common.struct.KanashiNode;
import ink.anur.core.SpringContextHolder;
import ink.anur.exception.NetWorkException;
import ink.anur.inject.NigateBean;

/**
 * Created by Anur IjuoKaruKas on 2020/3/7
 */
@NigateBean
public class ClientInetConfig implements InetConfig {

    private KanashiNode SERVER = null;

    private HashMap<String, KanashiNode> clusters = null;

    public void setClusters(List<KanashiNode> clusters) {
        HashMap<String, KanashiNode> m = new HashMap<>();
        for (KanashiNode cluster : clusters) {
            m.put(cluster.getServerName(), cluster);
        }
        this.clusters = m;
    }

    @NotNull
    @Override
    public String getLocalServerName() {
        return Constant.INSTANCE.getCLIENT();
    }

    @NotNull
    @Override
    public KanashiNode getNode(@Nullable String serverName) {
        if (Constant.INSTANCE.getSERVER()
                             .equals(serverName)) {
            if (SERVER == null) {
                KanashiConfig bean = SpringContextHolder.getBean(KanashiConfig.class);
                SERVER = new KanashiNode(Constant.INSTANCE.getSERVER(), bean.getHost(), bean.getPort());
            }
            return SERVER;
        } else {
            if (clusters == null) {
                throw new NetWorkException("还没有获取到集群信息代码却运行到了这里，估计是哪里有bug");
            } else {
                KanashiNode kanashiNode = clusters.get(serverName);
                if (kanashiNode == null) {
                    throw new NetWorkException("????");
                }
                return kanashiNode;
            }
        }
    }
}
