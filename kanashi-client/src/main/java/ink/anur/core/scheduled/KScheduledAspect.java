package ink.anur.core.scheduled;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.PostConstruct;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;
import ink.anur.core.response.ResponseProcessCentreService;
import ink.anur.inject.Nigate;
import ink.anur.inject.NigateInject;

/**
 * Created by Anur IjuoKaruKas on 2020/3/3
 */
@Component
public class KScheduledAspect {

    @Autowired
    private ApplicationContext appContext;

    @NigateInject
    private ResponseProcessCentreService responseProcessCentreService;

    private Map<String, Schedule> MAPPING = new HashMap<>();

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
                    MAPPING.put(simpleScheduled.name, simpleScheduled);
                }
            }
        }

        responseProcessCentreService.doSend()
    }

    public String methodNameGenerate(Method method) {
        return method.getDeclaringClass()
                     .getName() + "#" + method.getName();
    }
}
