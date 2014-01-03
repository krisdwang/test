package com.amazon.messaging.utils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;


public class TaskThreadFactory implements ThreadFactory {
    private final AtomicInteger threadNameCounter = new AtomicInteger(0);
    private final String initialThreadName;
    private final boolean critical;
    private final boolean daemon;
    
    public TaskThreadFactory(String initialThreadName, boolean critical) {
        this( initialThreadName, critical, true );
    }
    
    public TaskThreadFactory(String initialThreadName, boolean critical, boolean daemon) {
        this.initialThreadName = initialThreadName;
        this.critical = critical;
        this.daemon = daemon;
    }

    @Override
    public Thread newThread(Runnable r) {
        return new TaskThread(
                r, initialThreadName + "-" + threadNameCounter.incrementAndGet(), 
                critical, daemon );
    }
}
