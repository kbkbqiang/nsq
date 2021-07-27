package io.github.kbkbqiang.middleware.nsq;

import com.github.brainlag.nsq.NSQConfig;
import com.github.brainlag.nsq.NSQConsumer;
import com.github.brainlag.nsq.NSQProducer;
import com.github.brainlag.nsq.exceptions.NSQException;
import com.github.brainlag.nsq.lookup.DefaultNSQLookup;
import com.github.brainlag.nsq.lookup.NSQLookup;
import io.netty.handler.ssl.SslContext;

import javax.net.ssl.SSLContext;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

/**
 * @author zhaoqiang
 * @Description
 * @date 2018/10/7 下午5:49
 */
public class TestMain {

    public static void main(String[] args) throws NSQException, TimeoutException, NoSuchAlgorithmException {
        NSQProducer producer = new NSQProducer().addAddress("47.92.165.109", 10004).start();
        NSQConfig nsqConfig = new NSQConfig();
        SSLContext sslContext = SSLContext.getInstance("SSL");

        producer.setConfig(nsqConfig);
        producer.produce("TestTopic1", ("this is a message").getBytes());
        System.out.println("==================");

        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress("47.92.165.109", 10003);
        NSQConsumer consumer = new NSQConsumer(lookup, "TestTopic1", "dustin", (message) -> {
            System.out.println("received: " + message);
            //now mark the message as finished.
            message.finished();

            //or you could requeue it, which indicates a failure and puts it back on the queue.
            //message.requeue();
        });

        consumer.start();
    }
}
