package com.kbkbqiang.middleware.nsq.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @Auther zhaoqiang
 * @Date 2018/8/20 11:40
 * @Description
 */

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface NSQCloudMessageListener {

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
