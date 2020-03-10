package ink.anur.demo;

import org.springframework.stereotype.Component;
import ink.anur.core.scheduled.KScheduled;

/**
 * Created by Anur IjuoKaruKas on 2020/3/3
 * <p>
 * 简单的本地定时器 demo
 */
@Component
public class SimpleDemo {

    @KScheduled(cron = "*/10 * * * * *")
    public void print() {
        System.out.println("zzzzzzzz");
    }
}
