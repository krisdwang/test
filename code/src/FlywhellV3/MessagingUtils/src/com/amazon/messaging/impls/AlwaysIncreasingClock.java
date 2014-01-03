package com.amazon.messaging.impls;

import java.util.concurrent.atomic.AtomicLong;

import com.amazon.messaging.interfaces.Clock;

/**
 * A clock based on the system time that never goes backwards. If the system
 * time goes backwards this clock freezes in place until the system time
 * catches up again.
 * 
 * @author stevenso
 *
 */
public class AlwaysIncreasingClock implements Clock {
    private final AtomicLong maxTime;
    
    private final Clock baseClock;
    
    /**
     * Create an AlwaysIncreasingClock that starts with the current time.
     */
    public AlwaysIncreasingClock() {
        this(new SystemClock());
    }
    
    public AlwaysIncreasingClock(Clock baseClock) {
        this.baseClock = baseClock;
        this.maxTime = new AtomicLong( baseClock.getCurrentTime() );
    }
    
    /**
     * Create an AlwaysIncreasingClock that starts with the given time.
     */
    public AlwaysIncreasingClock(long initialTime) {
        baseClock = new SystemClock();
        maxTime = new AtomicLong( initialTime );
    }

    @Override
    public long getCurrentTime() {
        long baseTime = baseClock.getCurrentTime();
        long localMaxTime;
        do {
            localMaxTime = maxTime.get();
        } while( localMaxTime < baseTime && !maxTime.compareAndSet( localMaxTime, baseTime) );
        
        return maxTime.get();
    }
}
