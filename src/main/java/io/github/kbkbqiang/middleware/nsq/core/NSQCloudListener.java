package io.github.kbkbqiang.middleware.nsq.core;

public interface NSQCloudListener<T> {

    void onMessage(T message) throws Exception;
}
