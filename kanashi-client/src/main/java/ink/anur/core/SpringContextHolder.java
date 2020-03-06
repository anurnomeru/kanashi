package ink.anur.core;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

/**
 * Created by Anur IjuoKaruKas on 2020/3/6
 */
@Service
@Lazy(false)
public class SpringContextHolder implements ApplicationContextAware, DisposableBean {

    private static ApplicationContext applicationContext = null;

    /**
     *   * 取得存储在静态变量中的ApplicationContext.
     *  
     */
    public static ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    /**
     *      * 从静态变量applicationContext中取得Bean, 自动转型为所赋值对象的类型.
     *     
     */
    @SuppressWarnings("unchecked")
    public static <T> T getBeanByName(String name) {
        return (T) applicationContext.getBean(name);
    }

    /**
     *      * 从静态变量applicationContext中取得Bean, 自动转型为所赋值对象的类型.
     *     
     */
    public static <T> T getBean(Class<T> requiredType) {
        return applicationContext.getBean(requiredType);
    }

    /**
     *      * 清除SpringContextHolder中的ApplicationContext为Null.
     *     
     */
    public static void clearHolder() {
        applicationContext = null;
    }

    /**
     *      * 实现ApplicationContextAware接口, 注入Context到静态变量中.
     *     
     */
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        SpringContextHolder.applicationContext = applicationContext;
    }

    /**
     *      * 实现DisposableBean接口, 在Context关闭时清理静态变量.
     *     
     */
    @Override
    public void destroy() {
        SpringContextHolder.clearHolder();
    }
}
