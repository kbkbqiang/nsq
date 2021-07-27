package io.github.kbkbqiang.middleware.nsq.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * nsq配置
 * @author zhaoqiang
 * @create 2018-08-20 下午2:23
 **/

@Getter
@Setter
@ConfigurationProperties( prefix = "spring.nsq")
public class NSQProperties {


    private String serverAddrs;

    private String lookupAddrs;

    private int attempts = 5;


}
