package ink.anur.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Created by Anur IjuoKaruKas on 2020/3/4
 */
@Component
@ConfigurationProperties(prefix = "kanashi")
public class KanashiConfig {

    private Long reSendBackOffMs = 30L;
    private String host;
    private Integer port;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public Long getReSendBackOffMs() {
        return reSendBackOffMs;
    }

    public void setReSendBackOffMs(Long reSendBackOffMs) {
        this.reSendBackOffMs = reSendBackOffMs;
    }
}
