package com.amazon.messaging.utils;

import lombok.Synchronized;


public abstract class PeriodicService implements Runnable {

    private boolean running = false;
    
    private final Scheduler scheduler;

    private final String name;

    private final long interval;
    
    private final Object startLock = new Object();
    
    protected PeriodicService(Scheduler scheduler, String name, long interval) {
        this.scheduler = scheduler;
        this.name = name;
        this.interval = interval;
    }
    
    @Synchronized(value="startLock")
    public void start() {
        if (!running) {
            running = true;
            scheduler.executePeriodically(name, this, interval,false);
        }
    }

    @Synchronized(value="startLock")
    public void stop() {
        if (running) {
            running = false;
            scheduler.cancel(this, true, true);
        }
    }
    
}
