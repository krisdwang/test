package com.amazon.messaging.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazon.messaging.utils.ClosableStateManager.State;

import net.jcip.annotations.GuardedBy;

import static com.amazon.messaging.utils.ClosableStateManager.CloseableExceptionGenerator;

/**
 * This class implements a CompletionService that supports not just immediate events but
 * also events scheduled to occur in the future. It does not support repeating events.
 * <p>
 * The class also adds functions to support shutting down the CompletionService
 * and waiting for all tasks taht have been sent to the complete service to have
 * finished.
 * 
 * @author stevenso
 *
 */
public class ScheduledExecutorCompletionService<V> implements CompletionService<V> {
    private final ScheduledExecutorService executorService;
    private final BlockingQueue<Future<V>> completionQueue;
    private final AtomicInteger runningTasks = new AtomicInteger();
    private final Object shutdownNotifier = new Object();
    
    @GuardedBy("outstandingActions")
    private final Set<Future<V>> outstandingActions = Collections.newSetFromMap( 
            new IdentityHashMap<Future<V>, Boolean>());
    
    private final ClosableStateManager<RejectedExecutionException> stateManager = 
        new ClosableStateManager<RejectedExecutionException>(
            new CloseableExceptionGenerator<RejectedExecutionException>() {
                @Override
                public RejectedExecutionException getException(String operation, State state) {
                    return new RejectedExecutionException();
                }
            },
            false);

    public ScheduledExecutorCompletionService(ScheduledExecutorService executorService) {
        this.executorService = executorService;
        this.completionQueue = new LinkedBlockingQueue<Future<V>>();
    }

    public ScheduledExecutorCompletionService(ScheduledExecutorService executorService,
                                              BlockingQueue<Future<V>> completionQueue)
    {
        this.executorService = executorService;
        this.completionQueue = completionQueue;
    }

    /**
     * FutureTask extension to enqueue upon completion
     */
    private class QueueingFuture extends FutureTask<V> {
        QueueingFuture(Runnable task, V result) {
            super(task, result);
        }
        
        QueueingFuture(Callable<V> callable) {
            super(callable);
        }
        
        
        @Override
        public void run() {
            runningTasks.incrementAndGet();
            try {
                super.run();
            } finally {
                if( runningTasks.decrementAndGet() == 0 ) {
                    synchronized (shutdownNotifier) {
                        if( !stateManager.isOpen() ) shutdownNotifier.notifyAll();
                    }
                }
            }
        }
        
        @Override
        protected void done() {
            boolean noTasksLeft;
            
            completionQueue.add(this);
            synchronized (outstandingActions) {
                outstandingActions.remove(this);
                noTasksLeft = outstandingActions.isEmpty();
            }
            
            if( noTasksLeft ) {
                synchronized (shutdownNotifier) {
                    if( !stateManager.isOpen() ) shutdownNotifier.notifyAll();
                }
            }
        }
    }
    

    @Override
    public Future<V> submit(Callable<V> task) {
        QueueingFuture future = new QueueingFuture(task);
        
        stateManager.lockOpen("submit");
        try {
            synchronized (outstandingActions) {
                outstandingActions.add(future);
            }
            executorService.execute( future );
            return future;
        } finally {
            stateManager.unlock();
        }
    }

    @Override
    public Future<V> submit(Runnable task, V result) {
        QueueingFuture future = new QueueingFuture(task, result);
        
        stateManager.lockOpen("submit");
        try {
            executorService.execute( future );
            synchronized (outstandingActions) {
                outstandingActions.add(future);
            }
            return future;
        } finally {
            stateManager.unlock();
        }
    }
    
    /**
     * Submit a task for execution that will be executed after the given delay.
     * 
     * @param callable the function to executor
     * @param delay the time from now to delay execution
     * @param unit the time of the delay parameter
     * @return a future that can be used to get the result from callable
     */
    public Future<V> submitDelayed(Callable<V> callable, long delay, TimeUnit unit) {
        stateManager.lockOpen("submitDelayed");
        try {
            QueueingFuture future = new QueueingFuture(callable);
            executorService.schedule(future, delay, unit);
            synchronized (outstandingActions) {
                outstandingActions.add(future);
            }
            return future;
        } finally {
            stateManager.unlock();
        }
    }

    /**
     * Submit a task for execution that will be executed after the given delay.
     * 
     * @param command the task to execute
     * @param result the result to return from the future on successful execution
     * @param delay the time from now to delay execution
     * @param unit the time of the delay parameter
     * @return a future that can be used to get the result of command
     */
    public Future<V> submitDelayed(Runnable command, V result, long delay, TimeUnit unit) {
        stateManager.lockOpen("submitDelayed");
        try {
            QueueingFuture future = new QueueingFuture(command, result);
            executorService.schedule( future, delay, unit );
            synchronized (outstandingActions) {
                outstandingActions.add(future);
            }
            return future;
        } finally {
            stateManager.unlock();
        }
    }
    
    @Override
    public Future<V> take() throws InterruptedException {
        return completionQueue.take();
    }
    
    @Override
    public Future<V> poll() {
        return completionQueue.poll();
    }

    @Override
    public Future<V> poll(long timeout, TimeUnit unit) throws InterruptedException {
        return completionQueue.poll(timeout, unit );
    }
    
    /**
     * Cancel all tasks that haven't yet completed. Returns a list of the Futures for
     * those tasks.
     * 
     * @param interruptIfRunning if true any tasks currently running will have their thread interrupted
     * 
     * @return
     */
    public List<Future<V>> cancelAllOutstanding(boolean interruptIfRunning) {
        List<Future<V>> retval;
        synchronized (outstandingActions) {
            retval = new ArrayList<Future<V>>();
            retval.addAll( outstandingActions );
        }
        
        for( Future<V> future : retval ) {
            future.cancel(interruptIfRunning);
        }
        
        return retval;
    }
    
    private boolean hasOutstandingActions() {
        synchronized (outstandingActions) {
            return !outstandingActions.isEmpty();
        }
    }
    
    /**
     * Wait for all this ScheduledExecutorCompletionService to be shutdown and all tasks to be completed or canceled
     * and all threads currently running tasks to complete them.
     * 
     * @param timeout how long to wait for termination
     * @param unit the unit for timeout
     * @return true if all tasks were complete or canceled before the timeout occurred, else false
     * @throws InterruptedException
     */
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long endTime = MathUtil.addNoOverflow( System.currentTimeMillis(), unit.toMillis( timeout ) );
        synchronized (shutdownNotifier) {
            while( System.currentTimeMillis() < endTime && ( stateManager.isOpen() || runningTasks.get() != 0 || hasOutstandingActions() ) ) {
                shutdownNotifier.wait(Math.max(1, endTime - System.currentTimeMillis()));
            }
        }
        
        return !stateManager.isOpen() && runningTasks.get() == 0 && !hasOutstandingActions();
    }

    /**
     * Shutdown this ScheduledExecutorCompletionService. All new tasks submitted will throw 
     * a {@link RejectedExecutionException}. Currently scheduled and running tasks will continue 
     * to completion. To cancel remaining tasks call {@link #cancelAllOutstanding(boolean)} after 
     * calling shutdown.
     */
    public void shutdown() {
        if( stateManager.close() ) {
            synchronized (shutdownNotifier) {
                shutdownNotifier.notifyAll();
            }
        }
    }
    
    /**
     * Returns true if this ScheduledExecutorCompletionService has been shutdown
     * @return
     */
    public boolean isShutdown() {
        return !stateManager.isOpen();
    }
}
