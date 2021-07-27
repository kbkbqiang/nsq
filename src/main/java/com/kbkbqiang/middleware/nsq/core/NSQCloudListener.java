package com.kbkbqiang.middleware.nsq.core;

public interface NSQCloudListener<T> {

    void onMessage(T message) throws Exception;
}
