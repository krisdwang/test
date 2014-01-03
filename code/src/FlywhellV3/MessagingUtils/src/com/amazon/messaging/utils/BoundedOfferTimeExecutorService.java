package com.amazon.messaging.utils;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This executor changes {@link #execute(Runnable)} and the submit calls to wait for a 
 * a configured timeout when the queue is full instead of rejecting the submission immediately.
 * <p>
 * This executor is intended for processing tasks where it would be better to fail the 
 * task early if there are too many tasks pending but we still want some leeway to allow
 * requests to wait a while in the calling thread if the queue is full.
 * <p>
 * The timeout for the executor can be calculated from the time of the last successful enqueue
 * to allow rapidly rejecting tasks if execution has been stuck for a long time.
 * 
 * @author stevenso
 *
 */
public class BoundedOfferTimeExecutorService extends ThreadPoolExecutor {
    /** 
     * A RejectedExecutionHandler that attempts to add the task to the queue within 
     * maxWaitTimeNanos nanoseconds. If useLastSuccessTime is true then the
     * start time for the wait is the last successful offer, otherwise the start time
     * is the current time.
     * <p>
     * This class has a race condition that could cause it to incorrectly reject
     * a task if useLastSuccessTime is true, maxWaitTimeNanos has already passed since 
     * the last successful enqueue and then one thread successfully enqueues a task. A 
     * second thread could fail to enqueue and get an immediate rejection if runs before 
     * the first thread has a chance to update the last successful time. I don't think
     * that's a serious issue as nothing should be depending on a task going through
     * in those conditions.
     */
    private class BoundedTimeOfferHandler implements RejectedExecutionHandler {
        private final boolean useLastSuccessTime;
        
        private final long maxWaitTimeNanos;
        
        private final RejectedExecutionHandler nextHandler;
        
        private BoundedTimeOfferHandler(boolean useLastSuccessTime, long maxWaitTimeNanos,
                                        RejectedExecutionHandler nextHandler)
        {
            this.useLastSuccessTime = useLastSuccessTime;
            this.maxWaitTimeNanos = maxWaitTimeNanos;
            this.nextHandler = nextHandler;
        }

        @Override
        public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {
            if( executor.isShutdown() ) reject( task, executor, "Executor is shutdown." );
            
            long waitTime;
            
            if( useLastSuccessTime ) {
                waitTime = Math.max( 0, maxWaitTimeNanos - ( System.nanoTime() - lastSuccessfulEnqueueTime ) );
            } else {
                waitTime = maxWaitTimeNanos;
            }
            
            try {
                if( !executor.getQueue().offer(task, waitTime, TimeUnit.NANOSECONDS ) ) {
                    reject( task, executor, "Queue is full" );
                }
            } catch (InterruptedException e) {
                reject( task, executor, "Interrupted waiting for free slot" );
            }
        }
        
        private void reject( Runnable task, ThreadPoolExecutor executor, String message ) {
            if( nextHandler == null ) {
                throw new RejectedExecutionException(message);
            } else {
                nextHandler.rejectedExecution(task, executor);
            }
        }
    }
    
    private volatile long lastSuccessfulEnqueueTime;
    
    /**
     * Constructor for BoundedOfferTimeExecutorService.
     * 
     * @param corePoolSize the number of threads to keep in the
     *    pool, even if they are idle.
     * @param maximumPoolSize the maximum number of threads to allow in the
     *    pool.
     * @param keepAliveTime when the number of threads is greater than
     *    the core, this is the maximum time that excess idle threads
     *    will wait for new tasks before terminating.
     * @param keepAliveTimeUnit the time unit for the keepAliveTime
     *    argument.
     * @param maxQueueDepth the maximum queue depth allowed
     * @param maxOfferTime the maximum time to wait to add a job to the future
     * @param maxOfferTimeUnit the unit for maxOfferTime
     * @param waitStartsAtLastSuccessfullOffer if true the maximum wait when 
     *   adding to the queue will be maxOfferTime - ( currentTime - lastSuccesfullOfferTime ),
     *   otherwise it will always be maxOfferTime
     * @param threadBaseName the base name of the threads to use
     * @param daemon if true the threads created will be daemon threads
     */
    public BoundedOfferTimeExecutorService( 
        int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit keepAliveTimeUnit,
        int maxQueueDepth, long maxOfferTime, TimeUnit maxOfferTimeUnit,
        boolean waitStartsAtLastSuccessfullOffer, String threadBaseName, boolean daemon )
    {
        this( corePoolSize, maximumPoolSize, keepAliveTime, keepAliveTimeUnit, 
              maxQueueDepth, maxOfferTime, maxOfferTimeUnit, 
              waitStartsAtLastSuccessfullOffer,
              new TaskThreadFactory(threadBaseName, false, daemon ),
              null );
    }
        
    /**
     * Constructor for BoundedOfferTimeExecutorService.
     * 
     * @param corePoolSize the number of threads to keep in the
     *    pool, even if they are idle.
     * @param maximumPoolSize the maximum number of threads to allow in the
     *    pool.
     * @param keepAliveTime when the number of threads is greater than
     *    the core, this is the maximum time that excess idle threads
     *    will wait for new tasks before terminating.
     * @param keepAliveTimeUnit the time unit for the keepAliveTime
     *    argument.
     * @param maxQueueDepth the maximum queue depth allowed
     * @param maxOfferTime the maximum time to wait to add a job to the future
     * @param maxOfferTimeUnit the unit for maxOfferTime
     * @param waitStartsAtLastSuccessfullOffer if true the maximum wait when 
     *   adding to the queue will be maxOfferTime - ( currentTime - lastSuccesfullOfferTime ),
     *   otherwise it will always be maxOfferTime
     * @param threadFactory thread thread factory to use to create threads
     * @param rejectedExecutionHandler the {@link RejectedExecutionHandler} to call
     *   if the task can't be added to the queue within the timeout.
     */
    public BoundedOfferTimeExecutorService( 
        int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit keepAliveTimeUnit,
        int maxQueueDepth, long maxOfferTime, TimeUnit maxOfferTimeUnit,
        boolean waitStartsAtLastSuccessfullOffer, ThreadFactory threadFactory,
        RejectedExecutionHandler rejectedExecutionHandler )
    {
        super( corePoolSize, maximumPoolSize, keepAliveTime, keepAliveTimeUnit, 
               new ArrayBlockingQueue<Runnable>(maxQueueDepth), threadFactory );
        
        lastSuccessfulEnqueueTime = System.nanoTime();
        
        setRejectedExecutionHandler( 
                new BoundedTimeOfferHandler(
                        waitStartsAtLastSuccessfullOffer, 
                        maxOfferTimeUnit.toNanos(maxOfferTime),
                        rejectedExecutionHandler ) ); 
    }
    
    @Override
    public void execute(Runnable command) 
        throws RejectedExecutionException
    {
        super.execute(command);
        lastSuccessfulEnqueueTime = System.nanoTime();
    }
}
