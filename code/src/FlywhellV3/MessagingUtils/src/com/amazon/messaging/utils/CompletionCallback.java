package com.amazon.messaging.utils;

public interface CompletionCallback<T> {
    public void done(T result);
}
