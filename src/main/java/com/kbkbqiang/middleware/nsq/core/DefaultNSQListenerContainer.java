package com.kbkbqiang.middleware.nsq.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.brainlag.nsq.NSQConsumer;
import com.github.brainlag.nsq.NSQMessage;
import com.github.brainlag.nsq.lookup.DefaultNSQLookup;
import com.github.brainlag.nsq.lookup.NSQLookup;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author zhaoqiang
 * @create 2018-08-08 下午4:31
 **/
@Slf4j
public class DefaultNSQListenerContainer implements InitializingBean, NSQListenerContainer {

    @Setter
    @Getter
    private long suspendCurrentQueueTimeMillis = 1000;

    @Setter
    @Getter
    private String addrs;

    @Setter
    @Getter
    private String topic;

    @Setter
    @Getter
    private String channel;

    @Getter
    @Setter
    private String charset = "UTF-8";

    @Setter
    @Getter
    private ObjectMapper objectMapper = new ObjectMapper();

    @Setter
    @Getter
    private boolean started;

    @Setter
    private NSQListener nsqListener;

    private NSQConsumer consumer;

    private Class messageType;

    @Getter
    @Setter
    private boolean useTLS;

    @Getter
    @Setter
    private int attempts = 5;

    @Override
    public void setupMessageListener(NSQListener nsqListener) {
        this.nsqListener = nsqListener;
    }

    @Override
    public void destroy() {
        this.setStarted(false);
        if (Objects.nonNull(consumer)) {
            consumer.shutdown();
        }
        log.info("container destroyed, {}", this.toString());
    }

    public synchronized void start() {

        if (this.isStarted()) {
            throw new IllegalStateException("container already started. " + this.toString());
        }

        initNsqPushConsumer();

        // parse message type
        this.messageType = getMessageType();
        log.debug("msgType: {}", messageType.getName());

        consumer.start();
        this.setStarted(true);

        log.info("started container: {}", this.toString());
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        start();
    }

    @Override
    public String toString() {
        return "DefaultNSQListenerContainer{" +
                ", addrs='" + addrs + '\'' +
                ", topic='" + topic + '\'' +
                ", channel='" + channel + '\'' +
                ", charset='" + charset + '\'' +
                ", started=" + started +
                ", nsqListener=" + nsqListener +
                ", messageType=" + messageType +
                ", useTLS=" + useTLS +
                '}';
    }

    @SuppressWarnings("unchecked")
    private Object doConvertMessage(NSQMessage nsqMessage) {
        int currAttempts = nsqMessage.getAttempts();
        if (Objects.equals(messageType, NSQMessage.class)) {
            return nsqMessage;
        } else {
            String str = new String(nsqMessage.getMessage(), Charset.forName(charset));
            if (Objects.equals(messageType, String.class)) {
                return str;
            } else if (Objects.equals(messageType, List.class)) {
                return JSONArray.parseArray(str);
            } else {
                // if msgType not string, use objectMapper change it.
                try {
                    return JSONObject.toJavaObject(JSON.parseObject(str), messageType);
                } catch (Exception e) {
                    if (currAttempts > attempts) {
                        nsqMessage.finished();
                        log.error("max {} attempts is error:", attempts, e);
                    }
                    log.info("convert failed. str:{}, msgType:{}", str, messageType);
                    nsqMessage.setAttempts(currAttempts + 1);
                    throw new RuntimeException("cannot convert message to " + messageType, e);
                }
            }
        }

    }

    public String getLocalIp() {
        String host = null;
        try {
            host = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
        }
        return host;
    }


    private void initNsqPushConsumer() {

        Assert.notNull(addrs, "Property 'addrs' is required");
        Assert.notNull(nsqListener, "Property 'nsqListener' is required");
        Assert.notNull(topic, "Property 'topic' is required");
        Assert.notNull(channel, "Property 'channel' is required");

        NSQLookup lookup = new DefaultNSQLookup();

        String[] addrs = getAddrs().split(",");
        for (String addr : addrs) {
            String[] hosts = addr.split(":");
            lookup.addLookupAddress(hosts[0], Integer.valueOf(hosts[1]));
        }

        consumer = new NSQConsumer(lookup, topic, channel, message -> {
            int currAttempts = message.getAttempts();
            try {
                long now = System.currentTimeMillis();
                log.debug("received msg: {}", message);
                nsqListener.onMessage(doConvertMessage(message));
                message.finished();
                long costTime = System.currentTimeMillis() - now;
                log.debug("consume {} cost: {} ms", message.getAttempts(), costTime);
            } catch (Exception e) {
                if (currAttempts >= attempts) {
                    message.finished();
                    log.error("topic:{} max {} attempts is error:", topic, attempts, e);
                }
                message.setAttempts(currAttempts + 1);
                log.error("consume message failed. messageExt:", message, e);
                message.requeue();
            }
        });

    }

    private Class getMessageType() {
        Type[] interfaces = nsqListener.getClass().getGenericInterfaces();
        if (Objects.nonNull(interfaces)) {
            for (Type type : interfaces) {
                if (type instanceof ParameterizedType) {
                    ParameterizedType parameterizedType = (ParameterizedType) type;
                    if (Objects.equals(parameterizedType.getRawType(), NSQListener.class)) {
                        Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
                        if (Objects.isNull(actualTypeArguments) || actualTypeArguments.length == 0) {
                            return Object.class;
                        }

                        if (actualTypeArguments[0] instanceof ParameterizedType) {
                            ParameterizedType childType = (ParameterizedType) actualTypeArguments[0];
                            if (childType.getRawType().equals(List.class)) {
                                return List.class;
                            } else if (childType.getRawType().equals(Map.class)) {
                                return Map.class;
                            }
                        }

                        String typeName = actualTypeArguments[0].getTypeName();
                        if ("java.lang.String".equals(typeName)) {
                            return String.class;
                        }
                        return (Class) actualTypeArguments[0];
                    }
                }
            }

            return Object.class;
        } else {
            return Object.class;
        }
    }

}
