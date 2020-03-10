package ink.anur.core.scheduled;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.PostConstruct;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;
import ink.anur.common.Constant;
import ink.anur.core.response.ResponseProcessCentreService;
import ink.anur.inject.Event;
import ink.anur.inject.Nigate;
import ink.anur.inject.NigateInject;
import ink.anur.inject.NigateListener;
import ink.anur.pojo.client.ScheduledReport;
import ink.anur.pojo.client.SimpleScheduledMeta;

/**
 * Created by Anur IjuoKaruKas on 2020/3/3
 */
@Component
public class KScheduledAspect {

    @Autowired
    private ApplicationContext appContext;

    @NigateInject
    private ResponseProcessCentreService responseProcessCentreService;

    private Map<String, SimpleScheduled> SIMPLE_SCHEDULED_POOL = new HashMap<>();

    @PostConstruct
    public void initKSchedule() {
        Nigate.INSTANCE.registerToNigate(this, "KScheduledAspect");

        for (String beanDefinitionName : appContext.getBeanDefinitionNames()) {
            Object bean = appContext.getBean(beanDefinitionName);
            Method[] declaredMethods = ReflectionUtils.getDeclaredMethods(AopUtils.getTargetClass(bean));
            for (Method declaredMethod : declaredMethods) {
                KScheduled kScheduled;
                if ((kScheduled = declaredMethod.getAnnotation(KScheduled.class)) != null) {
                    String cron = kScheduled.cron();
                    SimpleScheduled simpleScheduled = new SimpleScheduled();
                    simpleScheduled.name = methodNameGenerate(declaredMethod);
                    simpleScheduled.bean = bean;
                    simpleScheduled.cron = cron;
                    simpleScheduled.method = declaredMethod;
                    SIMPLE_SCHEDULED_POOL.put(simpleScheduled.name, simpleScheduled);
                }
            }
        }
    }

    @NigateListener(onEvent = Event.REGISTER_TO_SERVER)
    public void sendScheduledReport() {
        ScheduledReport scheduledReport = new ScheduledReport();
        List<SimpleScheduledMeta> simpleScheduledList = scheduledReport.getSimpleScheduledList();
        for (SimpleScheduled value : SIMPLE_SCHEDULED_POOL.values()) {
            simpleScheduledList.add(new SimpleScheduledMeta(value.name, value.cron));
        }

        scheduledReport.prepare();
        responseProcessCentreService.doSend(Constant.INSTANCE.getSERVER(), scheduledReport);
    }

    public String methodNameGenerate(Method method) {
        return method.getDeclaringClass()
                     .getName() + "#" + method.getName();
    }
}
