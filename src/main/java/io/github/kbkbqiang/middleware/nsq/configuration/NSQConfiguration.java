package io.github.kbkbqiang.middleware.nsq.configuration;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.brainlag.nsq.NSQProducer;
import com.sproutsocial.nsq.Client;
import com.sproutsocial.nsq.Publisher;
import io.github.kbkbqiang.middleware.nsq.annotation.NSQCloudMessageListener;
import io.github.kbkbqiang.middleware.nsq.annotation.NSQMessageListener;
import io.github.kbkbqiang.middleware.nsq.core.DefaultNSQCloudListenerContainer;
import io.github.kbkbqiang.middleware.nsq.core.DefaultNSQListenerContainer;
import io.github.kbkbqiang.middleware.nsq.core.DefaultNSQListenerContainerConstants;
import io.github.kbkbqiang.middleware.nsq.core.NSQCloudListener;
import io.github.kbkbqiang.middleware.nsq.core.NSQListener;
import io.github.kbkbqiang.middleware.nsq.properties.NSQCloudProperties;
import io.github.kbkbqiang.middleware.nsq.properties.NSQProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.StandardEnvironment;

import javax.annotation.Resource;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;


/**
 * @Auther zhaoqiang
 * @Date 2018/8/20 下午2:23
 * @Description
 */
@Configuration
@EnableConfigurationProperties({NSQProperties.class, NSQCloudProperties.class})
@Order
@Slf4j
public class NSQConfiguration {

    @Autowired
    private NSQProperties nsqProperties;

    @Autowired
    private NSQCloudProperties nsqCloudProperties;

    @Bean
    @ConditionalOnClass(NSQProducer.class)
    @ConditionalOnMissingBean(NSQProducer.class)
    public NSQProducer nsqProducer() {
        if (StringUtils.isBlank(nsqProperties.getServerAddrs())) {
            return null;
        }
        String[] addrs = nsqProperties.getServerAddrs().split(",");
        NSQProducer producer = new NSQProducer();
        for (String addr : addrs) {
            String[] hosts = addr.split(":");
            producer.addAddress(hosts[0], Integer.valueOf(hosts[1]));
        }
        producer.start();
        return producer;
    }

    //新增消息模块推送nsq消息到云端（）
    @Bean
    @ConditionalOnClass(Publisher.class)
    public Publisher nsqProducerCloud() throws Exception {
        Client client = new Client();
        client.setAuthSecret(nsqCloudProperties.getAuthSecret());
        String serverAddrs = nsqCloudProperties.getServerAddrs();
        Publisher publisher = new Publisher(serverAddrs);
        if (StringUtils.isNotBlank(nsqCloudProperties.getAuthSecret())) {
            publisher = new Publisher(client, serverAddrs, "");
        }
        return publisher;
    }

    @Bean
    @ConditionalOnClass(ObjectMapper.class)
    @ConditionalOnMissingBean(name = "nsqMessageObjectMapper")
    public ObjectMapper nsqMessageObjectMapper() {
        return new ObjectMapper();
    }


    @Configuration
    @Order
    public static class ListenerContainerConfiguration implements ApplicationContextAware, InitializingBean {

        private ConfigurableApplicationContext applicationContext;

        private AtomicLong counter = new AtomicLong(0);

        @Resource
        private StandardEnvironment environment;

        @Autowired
        private NSQProperties nsqProperties;

        private ObjectMapper objectMapper;

        public ListenerContainerConfiguration() {
        }

        @Autowired(required = false)
        public ListenerContainerConfiguration(@Qualifier("nsqMessageObjectMapper") ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
        }

        @Override
        public void afterPropertiesSet() throws Exception {
            Map<String, Object> beans = this.applicationContext.getBeansWithAnnotation(NSQMessageListener.class);

            if (Objects.nonNull(beans)) {
                beans.forEach(this::registerContainer);
            }
        }

        private void registerContainer(String beanName, Object bean) {
            Class<?> clazz = AopUtils.getTargetClass(bean);

            if (!NSQListener.class.isAssignableFrom(bean.getClass())) {
                throw new IllegalStateException(clazz + " is not instance of " + NSQListener.class.getName());
            }

            NSQListener nsqListener = (NSQListener) bean;
            NSQMessageListener annotation = clazz.getAnnotation(NSQMessageListener.class);
            BeanDefinitionBuilder beanBuilder = BeanDefinitionBuilder.rootBeanDefinition(DefaultNSQListenerContainer.class);
            beanBuilder.addPropertyValue(DefaultNSQListenerContainerConstants.PROP_CONSUMER_ADDRES, nsqProperties.getLookupAddrs());
            beanBuilder.addPropertyValue(DefaultNSQListenerContainerConstants.PROP_TOPIC, environment.resolvePlaceholders(annotation.topic()));
            beanBuilder.addPropertyValue(DefaultNSQListenerContainerConstants.PROP_CHANNEL, environment.resolvePlaceholders(annotation.channel()));
            beanBuilder.addPropertyValue(DefaultNSQListenerContainerConstants.PROP_ATTEMPTS, nsqProperties.getAttempts());
            beanBuilder.addPropertyValue(DefaultNSQListenerContainerConstants.PROP_NSQ_LISTENER, nsqListener);
            if (Objects.nonNull(objectMapper)) {
                beanBuilder.addPropertyValue(DefaultNSQListenerContainerConstants.PROP_OBJECT_MAPPER, objectMapper);
            }
            beanBuilder.setDestroyMethodName(DefaultNSQListenerContainerConstants.METHOD_DESTROY);
            String containerBeanName = String.format("%s_%s", DefaultNSQListenerContainer.class.getName(), counter.incrementAndGet());
            DefaultListableBeanFactory beanFactory = (DefaultListableBeanFactory) applicationContext.getBeanFactory();
            beanFactory.registerBeanDefinition(containerBeanName, beanBuilder.getBeanDefinition());

            DefaultNSQListenerContainer container = beanFactory.getBean(containerBeanName, DefaultNSQListenerContainer.class);

            if (!container.isStarted()) {
                try {
                    container.start();
                } catch (Exception e) {
                    log.error("started container failed. {}", container, e);
                    throw new RuntimeException(e);
                }
            }
            log.info("register nsq listener to container, listenerBeanName:{}, containerBeanName:{}", beanName, containerBeanName);

        }

        @Override
        public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
            this.applicationContext = (ConfigurableApplicationContext) applicationContext;
        }
    }


    @Configuration
    @Order
    public static class CloudListenerContainerConfiguration implements ApplicationContextAware, InitializingBean {

        private ConfigurableApplicationContext applicationContext;

        private AtomicLong counter = new AtomicLong(0);

        @Resource
        private StandardEnvironment environment;

        @Autowired
        private NSQCloudProperties nsqCloudProperties;

        private ObjectMapper objectMapper;

        public CloudListenerContainerConfiguration() {
        }

        @Autowired(required = false)
        public CloudListenerContainerConfiguration(@Qualifier("nsqMessageObjectMapper") ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
        }

        @Override
        public void afterPropertiesSet() throws Exception {
            Map<String, Object> beans = this.applicationContext.getBeansWithAnnotation(NSQCloudMessageListener.class);

            if (Objects.nonNull(beans)) {
                beans.forEach(this::registerContainer);
            }
        }

        private void registerContainer(String beanName, Object bean) {
            Class<?> clazz = AopUtils.getTargetClass(bean);

            if (!NSQCloudListener.class.isAssignableFrom(bean.getClass())) {
                throw new IllegalStateException(clazz + " is not instance of " + NSQCloudListener.class.getName());
            }

            NSQCloudListener nsqCloudListener = (NSQCloudListener) bean;
            NSQCloudMessageListener annotation = clazz.getAnnotation(NSQCloudMessageListener.class);
            BeanDefinitionBuilder beanBuilder = BeanDefinitionBuilder.rootBeanDefinition(DefaultNSQCloudListenerContainer.class);
            beanBuilder.addPropertyValue(DefaultNSQListenerContainerConstants.PROP_CONSUMER_ADDRES, nsqCloudProperties.getServerAddrs());
            beanBuilder.addPropertyValue(DefaultNSQListenerContainerConstants.PROP_TOPIC, environment.resolvePlaceholders(annotation.topic()));
            beanBuilder.addPropertyValue(DefaultNSQListenerContainerConstants.PROP_CHANNEL, environment.resolvePlaceholders(annotation.channel()));
            beanBuilder.addPropertyValue(DefaultNSQListenerContainerConstants.PROP_ATTEMPTS, nsqCloudProperties.getAttempts());
            beanBuilder.addPropertyValue(DefaultNSQListenerContainerConstants.PROP_NSQ_CLOUD_LISTENER, nsqCloudListener);
            beanBuilder.addPropertyValue(DefaultNSQListenerContainerConstants.AUTHSECRET, nsqCloudProperties.getAuthSecret());
            if (Objects.nonNull(objectMapper)) {
                beanBuilder.addPropertyValue(DefaultNSQListenerContainerConstants.PROP_OBJECT_MAPPER, objectMapper);
            }
            beanBuilder.setDestroyMethodName(DefaultNSQListenerContainerConstants.METHOD_DESTROY);
            String containerBeanName = String.format("%s_%s", DefaultNSQCloudListenerContainer.class.getName(), counter.incrementAndGet());
            DefaultListableBeanFactory beanFactory = (DefaultListableBeanFactory) applicationContext.getBeanFactory();
            beanFactory.registerBeanDefinition(containerBeanName, beanBuilder.getBeanDefinition());

            DefaultNSQCloudListenerContainer container = beanFactory.getBean(containerBeanName, DefaultNSQCloudListenerContainer.class);

            if (!container.isStarted()) {
                try {
                    container.start();
                } catch (Exception e) {
                    log.error("started container failed. {}", container, e);
                    throw new RuntimeException(e);
                }
            }
            log.info("register nsq listener to container, listenerBeanName:{}, containerBeanName:{}", beanName, containerBeanName);

        }

        @Override
        public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
            this.applicationContext = (ConfigurableApplicationContext) applicationContext;
        }
    }
}
