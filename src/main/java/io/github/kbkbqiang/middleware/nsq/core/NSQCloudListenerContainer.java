package io.github.kbkbqiang.middleware.nsq.core;

import org.springframework.beans.factory.DisposableBean;

public interface NSQCloudListenerContainer extends DisposableBean {

    /**
     * Setup the message listener to use. Throws an {@link IllegalArgumentException} if that message listener type is
     * not supported.
     *
     * @param messageListener see {@link NSQCloudListener}
     */
    void setupMessageListener(NSQCloudListener<?> messageListener);
}
