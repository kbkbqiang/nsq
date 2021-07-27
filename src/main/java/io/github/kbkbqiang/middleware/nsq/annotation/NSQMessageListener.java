package io.github.kbkbqiang.middleware.nsq.annotation;

import java.lang.annotation.*;

/**
 * @Auther zhaoqiang
 * @Date 2018/8/20 11:40
 * @Description
 */

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface NSQMessageListener {

    /**
     * Topic name
     * @return
     */
    String topic();


    /**
     * channel name
     * @return
     */
    String channel();

}
