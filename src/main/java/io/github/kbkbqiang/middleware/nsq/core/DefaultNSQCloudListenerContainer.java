package io.github.kbkbqiang.middleware.nsq.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.brainlag.nsq.NSQMessage;
import com.sproutsocial.nsq.Client;
import com.sproutsocial.nsq.DirectSubscriber;
import com.sproutsocial.nsq.Message;
import com.sproutsocial.nsq.MessageHandler;
import com.sproutsocial.nsq.Subscriber;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
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
public class DefaultNSQCloudListenerContainer implements InitializingBean, NSQCloudListenerContainer {

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

    @Setter
    @Getter
    private String authSecret;

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
    private NSQCloudListener nsqCloudListener;

    private Subscriber subscriber;

    private Class messageType;

    @Getter
    @Setter
    private boolean useTLS;

    @Getter
    @Setter
    private int attempts = 5;

    @Override
    public void setupMessageListener(NSQCloudListener nsqCloudListener) {
        this.nsqCloudListener = nsqCloudListener;
    }

    @Override
    public void destroy() {
        this.setStarted(false);
        if (Objects.nonNull(subscriber)) {
            subscriber.stop();
        }
        log.info("container destroyed, {}", this.toString());
    }

    public synchronized void start() {

        if (this.isStarted()) {
            throw new IllegalStateException("container already started. " + this.toString());
        }

        initNsqCloudPushConsumer();

        // parse message type
        this.messageType = getMessageType();
        log.debug("msgType: {}", messageType.getName());
        this.setStarted(true);

        log.info("started container: {}", this.toString());
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        start();
    }

    @Override
    public String toString() {
        return "DefaultNSQCloudListenerContainer{" +
                ", addrs='" + addrs + '\'' +
                ", topic='" + topic + '\'' +
                ", channel='" + channel + '\'' +
                ", charset='" + charset + '\'' +
                ", started=" + started +
                ", nsqListener=" + nsqCloudListener +
                ", messageType=" + messageType +
                ", useTLS=" + useTLS +
                ", authSecret=" + authSecret +
                '}';
    }

    @SuppressWarnings("unchecked")
    private Object doConvertMessage(Message message) {
        int currAttempts = message.getAttempts();
        if (Objects.equals(messageType, Message.class)) {
            return message;
        } else {
            String str = new String(message.getData(), Charset.forName(charset));
            if (Objects.equals(messageType, String.class)) {
                return str;
            } else if (Objects.equals(messageType, List.class)) {
                return JSONArray.parseArray(str);
            } else if (Objects.equals(messageType, Byte[].class)) {
                return message.getData();
            } else {
                // if msgType not string, use objectMapper change it.
                try {
                    return JSONObject.toJavaObject(JSON.parseObject(str), messageType);
                } catch (Exception e) {
                    if (currAttempts > attempts) {
                        message.finish();
                        log.error("max {} attempts is error:", attempts, e);
                    }
                    log.info("convert failed. str:{}, msgType:{}", str, messageType);
                    message.requeue();
                    throw new RuntimeException("cannot convert message to " + messageType, e);
                }
            }
        }

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


    private void initNsqCloudPushConsumer() {

        Assert.notNull(addrs, "Property 'addrs' is required");
        Assert.notNull(nsqCloudListener, "Property 'nsqListener' is required");
        Assert.notNull(topic, "Property 'topic' is required");
        Assert.notNull(channel, "Property 'channel' is required");
        Assert.notNull(authSecret, "Property 'authSecret' is required");


        Client defaultClient = Client.getDefaultClient();
        if (StringUtils.isNotBlank(authSecret)) {
            defaultClient.setAuthSecret(authSecret);
        }

        subscriber = new DirectSubscriber(2, addrs);

        subscriber.subscribe(topic, channel, (MessageHandler) message -> {

//            Base64 base64 = new Base64();
//            String data = JSON.toJSONString(message.getData());
//            System.out.println("====" + JSON.toJSONString(message.getData()));
//            try {
//                System.out.println("----" + new String(base64.decode(data), "UTF-8"));
//            } catch (UnsupportedEncodingException e) {
//                e.printStackTrace();
//            }
//            message.requeue();
            long now = System.currentTimeMillis();
            try {
                log.debug("received msg: {}", message);
                nsqCloudListener.onMessage(doConvertMessage(message));
                message.finish();
                long costTime = System.currentTimeMillis() - now;
                log.debug("consume {} cost: {} ms", message.getAttempts(), costTime);
            } catch (Exception e) {
                message.requeue();
                log.error("consume message failed. messageExt:", message, e);
            }
        });

    }

    @SuppressWarnings("unchecked")
    private Object doConvertMessage(byte[] msg) {
        if (Objects.equals(messageType, NSQMessage.class)) {
            return msg;
        } else {
            String str = new String(msg, Charset.forName(charset));
            if (Objects.equals(messageType, String.class)) {
                return str;
            } else if (Objects.equals(messageType, List.class)) {
                return JSONArray.parseArray(str);
            } else {
                // if msgType not string, use objectMapper change it.
                try {
                    return JSONObject.toJavaObject(JSON.parseObject(str), messageType);
                } catch (Exception e) {
                    log.info("convert failed. str:{}, msgType:{}", str, messageType);
//                    nsqMessage.setAttempts(currAttempts + 1);
                    throw new RuntimeException("cannot convert message to " + messageType, e);
                }
            }
        }

    }

    private Class getMessageType() {
        Type[] interfaces = nsqCloudListener.getClass().getGenericInterfaces();
        if (Objects.nonNull(interfaces)) {
            for (Type type : interfaces) {
                if (type instanceof ParameterizedType) {
                    ParameterizedType parameterizedType = (ParameterizedType) type;
                    if (Objects.equals(parameterizedType.getRawType(), NSQCloudListener.class)) {
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
