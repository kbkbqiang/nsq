# nsq
NSQ是Java编写的NSQ消息中间件
NSQ提供自动连接插件，通过Java注解监听服务消息

####添加 pom.xml配置
```xml
<dependency>
    <groupId>com.kbkbqiang</groupId>
    <artifactId>nsq</artifactId>
    <version>1.0.1.RELEASE</version>
</dependency>
```
####配置 application.yaml文件
````yaml
spring:
  nsqcloud:
    serverAddrs: 127.0.0.1:4150		#订阅地址
    lookupAddrs: 127.0.0.1:4150		#订阅地址
    attempts: 10					#失败尝试次数
    isTls: false					#是否启用tsl连接
    keyCertChainFilePemPath: 		#tsl 文件地址
    keyFilePemPath: 				#tsl 文件地址
    trustCertCollectionFilePemPath: #tsl 文件地址
    password: 						#tsl 文件密码
    authSecret: 12345212			#连接密钥
````

####程序内部使用
```java
@Slf4j
@Component
@NSQCloudMessageListener(topic = "topic_name", channel = "test_channel")
public class TslDataResListener implements NSQCloudListener<Message> {

    @Override
    public void onMessage(Message message) throws Exception {
        log.info("topic:topic_name json:{}", JSON.toJSONString(message));
    }
}

```