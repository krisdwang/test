package com.amazon.messaging.concurent;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.RequiredArgsConstructor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * A future that is marked as done once all of the futures added to it 
 * are done. Any added futures that failed still count as completed. The
 * reportException method will be called with any ExecutorException's reported
 * by added futures.
 * 
 * @author stevenso
 *
 */
public class AllCompletedFuture<T> extends AbstractFuture<Void> {
    private static final Log log = LogFactory.getLog(AllCompletedFuture.class);
    
    private static final Executor sameThreadExecutor = MoreExecutors.sameThreadExecutor();
    
    @RequiredArgsConstructor
    private class Callback implements Runnable {
        private final T object;
        private final ListenableFuture<?> future;
        
        @Override
        public void run() {
            futureCompleted();

            boolean interrupted = false;
            try {
                while(true){
                    try {
                        future.get();
                        return;
                    } catch( InterruptedException e ) {
                        interrupted = true;
                    }
                }
            } catch (ExecutionException e) {
                reportException(object, e);
            } finally {
                if( interrupted ) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
    
    private final AtomicInteger numRemainingFutures = new AtomicInteger();
    private volatile boolean allFuturesAdded = false;
    
    /**
     * Add a future to include in the set. 
     * 
     * @param future the future to add
     */
    public void addFuture(ListenableFuture<?> future) {
        addFuture(null, future);
    }
    
    /**
     * Add a future to include in the set. 
     * 
     * @param object an object associated with the future. This will passed
     *  to reportException if the future reports an error 
     * @param future the future to add
     */
    public void addFuture(T object, ListenableFuture<?> future) {
        Preconditions.checkState(!allFuturesAdded);
        Preconditions.checkNotNull(future);
        
        numRemainingFutures.incrementAndGet();
        future.addListener(new Callback(object, future), sameThreadExecutor);
    }
    
    /**
     * Call this after all futures have been added using addFuture.
     */
    public void finishedAddingFutures() {
        Preconditions.checkState(!allFuturesAdded);
        
        allFuturesAdded = true;
        if( numRemainingFutures.get() == 0) {
            // Could be a double set - we don't care if it is
            set(null);
        }
    }
    
    private void futureCompleted() {
        int remaining = numRemainingFutures.decrementAndGet();
        if( allFuturesAdded && remaining == 0 ) {
            set(null);
        }
    }
    
    /**
     * Report that an exception happened for one of the added futures. This function
     * just logs the result at an warn level
     * @param object 
     */
    protected void reportException(T object, ExecutionException e) {
        if( object != null ) {
            log.warn( "Got exception from " + object, e );
        } else {
            log.warn( "Got exception", e );
        }
    }
}
