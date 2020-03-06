package ink.anur.core;

import java.lang.reflect.Method;
import java.util.Map;
import javax.annotation.PostConstruct;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;

/**
 * Created by Anur IjuoKaruKas on 2020/3/3
 */
@Component
public class KScheduledAspect {

    @Autowired
    private ApplicationContext appContext;

//    @PostConstruct
//    public void initKSchedule() {
//        for (String beanDefinitionName : appContext.getBeanDefinitionNames()) {
//            Object bean = appContext.getBean(beanDefinitionName);
//            Method[] declaredMethods = ReflectionUtils.getDeclaredMethods(AopUtils.getTargetClass(bean));
//            for (Method declaredMethod : declaredMethods) {
//                if (declaredMethod.getAnnotation(KScheduled.class) != null) {
//                }
//            }
//        }
//    }
}
