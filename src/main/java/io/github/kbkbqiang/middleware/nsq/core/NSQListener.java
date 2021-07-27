package io.github.kbkbqiang.middleware.nsq.core;

public interface NSQListener<T> {
    void onMessage(T message) throws Exception;
}
