package ink.anur.core.scheduled;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created by Anur IjuoKaruKas on 2020/3/3
 */
@Target({java.lang.annotation.ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface KScheduled {

    String cron();

    boolean mutex() default false;
}
