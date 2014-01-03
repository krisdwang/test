package com.amazon.messaging.impls;

import java.util.concurrent.atomic.AtomicLong;

import com.amazon.messaging.interfaces.Clock;

/**
 * A clock that can be set to any time you specify
 * @author stevenso
 *
 */
public class SettableClock implements Clock {
    private AtomicLong time;
    
    /**
     * Create a settable clock with the default time of 1.
     */
    public SettableClock() {
        time = new AtomicLong(1);
    }
    
    /**
     * Create a settable clock that starts at time initialTime
     * @param initialTime
     */
    public SettableClock(long initialTime) {
        time = new AtomicLong( initialTime );
    }
    
    public void setCurrentTime(long time) {
        this.time.set( time );
    }
    
    public void increaseTime(long interval) {
        time.addAndGet( interval );
    }
    
    @Override
    public long getCurrentTime() {
        return time.get();
    }
    
    @Override
    public String toString() {
        return Long.toString( time.get() );
    }
}
