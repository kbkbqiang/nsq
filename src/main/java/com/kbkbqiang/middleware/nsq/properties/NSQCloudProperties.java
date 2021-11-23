package com.kbkbqiang.middleware.nsq.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * nsq云端配置
 **/

@Getter
@Setter
@ConfigurationProperties(prefix = "spring.nsqcloud")
public class NSQCloudProperties {


    private String serverAddrs;

    private String lookupAddrs;

    private int attempts = 10;

    private boolean isTls = false;

    private String keyCertChainFilePemPath;

    private String keyFilePemPath;

    private String trustCertCollectionFilePemPath;

    private String password;

    private String authSecret;
}
