package com.amazon.messaging.utils;

import javax.annotation.concurrent.GuardedBy;

import com.amazon.messaging.interfaces.Clock;

/**
 * Utility class to help with implementing exponential backoff.
 * 
 * @author stevenso
 */
public class ExponentialBackoff {
    private final Object backoffIntervalLock = new Object();
    
    private final long initialBackoffInterval;
    private final long maxBackoffInterval;
    private final double backoffIntervalMultiplier;
    private final Clock clock;
    
    private volatile long nextTryTime;
    
    @GuardedBy("backoffIntervalLock")
    private long currentBackoffInterval;
    
    public ExponentialBackoff(long initialBackoffInterval, long maxBackoffInterval,
                              double backoffIntervalMultiplier, Clock clock)
    {
        super();
        this.initialBackoffInterval = initialBackoffInterval;
        this.maxBackoffInterval = maxBackoffInterval;
        this.backoffIntervalMultiplier = backoffIntervalMultiplier;
        this.clock = clock;
        
        nextTryTime = 0;
        currentBackoffInterval = initialBackoffInterval;
    }

    /**
     * Reset the backoff interval to the initial backoff interval
     */
    public void resetBackoffInterval() {
        synchronized (backoffIntervalLock) {
            currentBackoffInterval = initialBackoffInterval;
        }
    }
    
    /**
     * Return true if the next retry time is in the future.
     */
    public boolean waitingToRetry() {
        return clock.getCurrentTime() < nextTryTime;
    }
    
    /**
     * Return how long till the next retry
     * @return
     */
    public long getTimeTillNextTry() {
        return Math.max( 0, nextTryTime - clock.getCurrentTime() );
    }
    
    /**
     * Get how long in milliseconds until the next retry should occur.
     * @return
     */
    public long getNextTryTime() {
        return nextTryTime;
    }
    
    /**
     * Manually set the next retry time. This does not change the backoff
     * interval.
     */
    public void setNextRetryTime(long time) {
        nextTryTime = time;
    }
    
    /**
     * Record a failure. Does nothing if the next retry time is already in the future.
     * Otherwise the next try time is set to the current time plus the current backoff 
     * interval and the current backoff interval is increased to the minimum of its 
     * old interval times the multiplier and the maximum allowed backoff interval.   
     * 
     * @return true if this call moved the next try time into the future.
     */
    public boolean recordFailure() {
        long currentTime = clock.getCurrentTime();
        
        synchronized (backoffIntervalLock) {
            if( nextTryTime > currentTime ) return false;
            
            nextTryTime = currentTime + currentBackoffInterval;
            
            currentBackoffInterval =
                Math.min( ( long ) ( currentBackoffInterval * backoffIntervalMultiplier ), maxBackoffInterval );
            
            return true;
        }
    }
}
