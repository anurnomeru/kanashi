package ink.anur.core.scheduled;

import java.lang.reflect.Method;

/**
 * Created by Anur IjuoKaruKas on 2020/3/10
 */
public class SimpleScheduled implements Schedule {
    public String cron;
    public Method method;
    public Object bean;
    public String name;

    @Override
    public String name() {
        return name;
    }
}
