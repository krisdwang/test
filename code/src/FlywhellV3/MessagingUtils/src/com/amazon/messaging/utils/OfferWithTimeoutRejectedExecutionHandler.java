package com.amazon.messaging.utils;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This RejectedExecutionHandler attempts to add to the queue with a timeout. If the timeout
 * expires before an entry becomes available in the queue it throws a 
 * {@link RejectedExecutionException}.
 * 
 * @author stevenso
 *
 */
public class OfferWithTimeoutRejectedExecutionHandler implements RejectedExecutionHandler {
    private final long offerTime;
    
    private final TimeUnit offerTimeUnit;
    
    public OfferWithTimeoutRejectedExecutionHandler(long offerTime, TimeUnit offerTimeUnit) {
        this.offerTime = offerTime;
        this.offerTimeUnit = offerTimeUnit;
    }

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        if( executor.isShutdown() ) throw new RejectedExecutionException("Executor is shutdown");
        
        try {
            if( !executor.getQueue().offer(r, offerTime, offerTimeUnit ) ) {
                throw new RejectedExecutionException("Queue is full");
            }
        } catch (InterruptedException e) {
            throw new RejectedExecutionException("Interrupted waiting for free slot", e);
        }
    }
    
}