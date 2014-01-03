/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amazon.messaging.utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.umd.cs.findbugs.annotations.CheckReturnValue;
import edu.umd.cs.findbugs.annotations.SuppressWarnings;

/**
 * Statically schedule tasks to run in the background.
 * 
 * @author kaitchuc
 */
public final class Scheduler {

    private static final Log log = LogFactory.getLog(Scheduler.class);

    private final String inactiveName;
    
    private boolean requireCleanShutdown;
    
    enum defaultInstance {
        GLOBAL;
        final Scheduler scheduler;
        
        @SuppressWarnings("synthetic-access")
        defaultInstance() {
            scheduler = new Scheduler("Inactive MessagingUtils Scheduler thread", 7);
        }
    }

    /**
     * Provides a cancellable task that can also be waited on until task
     * completion.
     * 
     * @author Modifications by robburke based on RunnableTask
     */
    private class SmartRunnable implements Runnable {

        final String name;

        final Runnable realRunnable;

        final long period;

        ScheduledFuture<?> task;

        Object mutex = new Object();

        int running = 0;

        public SmartRunnable(String name, Runnable realRunnable, long period) {
            super();
            this.name = name;
            this.realRunnable = realRunnable;
            this.period = period;
        }

        @Override
        public void run() {
            Thread.currentThread().setName(name);
            try {
                synchronized (mutex) {
                    running++;
                }
                realRunnable.run();
            } catch (Exception e) {
                log.error("Failed to run " + name, e);
            } catch (AssertionError e) {
                log.error("Assertion error running " + name, e);
            } finally {
                synchronized (mutex) {
                    running--;
                    mutex.notifyAll();
                }
            }

            if (period == -1) {
                synchronized (Scheduler.this) {
                    smartRunners.remove(realRunnable);
                }
            }

            Thread.currentThread().setName(inactiveName);
        }

        public void waitUntilNotRunning() throws InterruptedException {
            synchronized (mutex) {
                while (running != 0) {
                    mutex.wait();
                }
            }
        }
    }
    
    private final ScheduledExecutorService pool;

    private final HashMap<Runnable, SmartRunnable> smartRunners = new HashMap<Runnable, SmartRunnable>();
    
    public Scheduler(String inactiveThreadName, ScheduledExecutorService executor) {
        inactiveName = inactiveThreadName;
        pool = executor;
    }

    public Scheduler(String inactiveThreadName, int numberOfThreads) {
        inactiveName = inactiveThreadName;
        pool = createPool(inactiveThreadName, numberOfThreads);
    }
    
    /**
     * Set if a clean shutdown is required. If true calling shutdown while there are still tasks
     * scheduled will throw {@link IllegalStateException}. This is intended for unit testing
     * and probably shouldn't be used in production.
     * 
     * @param requireCleanShutdown if clean shutdown is required
     */
    public void setRequireCleanShutdown(boolean requireCleanShutdown) {
        this.requireCleanShutdown = requireCleanShutdown;
    }
    
    public boolean isRequireCleanShutdown() {
        return requireCleanShutdown;
    }
    
    private static ScheduledThreadPoolExecutor createPool(String inactiveThreadName, int numberOfThreads) {
        ScheduledThreadPoolExecutor retval =
            new ScheduledThreadPoolExecutor(
                    numberOfThreads, 
                new TaskThreadFactory(inactiveThreadName, true ) );
        
        retval.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        retval.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        
        return retval;
    }

    public synchronized void executePeriodically(String name, final Runnable task, long period,
            boolean immediately) 
    {
        if( pool.isShutdown() ) {
            log.error( "Attempt to schedule new task on shutdown scheduler: name = " + name + " task = " + task, 
                    new RuntimeException("Attempt to schedule new task on shutdown scheduler") );
            return;
        }
        
        SmartRunnable run = new SmartRunnable(name, task, period);
        smartRunners.put(task, run);
        ScheduledFuture<?> ticket = pool.scheduleWithFixedDelay(run, (immediately) ? 0 : period, period,
                TimeUnit.MILLISECONDS);
        run.task = ticket;
    }

    /**
     * Cancel the given task and wait until it's completed. Does not block other
     * scheduler requests while waiting for a task to finish.
     * 
     * @param task
     * @return If the task was canceled.
     */
    public boolean cancel(Runnable task, boolean interupt, boolean wait) {
        boolean canceled = false;
        SmartRunnable run = null;

        int smartRunnersSize;
        synchronized (Scheduler.this) {
            run = smartRunners.remove(task);
            if (run != null) {
                canceled = run.task.cancel(interupt);
            } else {
                log.warn("Task not found in scheduler: " + task);
            }
            smartRunnersSize = smartRunners.size(); 
        }
        
        if( canceled && pool instanceof ThreadPoolExecutor ) {
            ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) pool;
            if( threadPoolExecutor.getQueue().size() >= 2 * smartRunnersSize ) {
                // Don't run purge on every cancel as that results in O(n^2) performance
                //  when canceling all tasks. Doing it only when there have been at least
                //  n/2 tasks cancelled results in O(n * ( log n ) ^ 2 ) performance.
                threadPoolExecutor.purge();
            }
        }
        
        try {
            if (wait && (run != null)) {
                run.waitUntilNotRunning();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return canceled ;
    }

    public synchronized void executeAfterDelay(String name, final Runnable task, long delay) {
        if( pool.isShutdown() ) {
            log.error( "Attempt to schedule new task on shutdown scheduler: name = " + name + " task = " + task, 
                    new RuntimeException("Attempt to schedule new task on shutdown scheduler") );
            return;
        }
        
        SmartRunnable run = new SmartRunnable(name, task, -1);
        ScheduledFuture<?> ticket = pool.schedule(run, delay, TimeUnit.MILLISECONDS);
        run.task = ticket;
        smartRunners.put(task, run);
    }

    /**
     * Reschedules the next execution of a periodic task to occur after the
     * specified delay. If interrupt is true and the task is currently running
     * then it will be interrupted.
     * 
     * @param task
     *            the task to adjust the schedule for
     * @param delay
     *            the delay in ms before the next run of task should occur
     * @return true if task is successfully scheduled
     */
    @CheckReturnValue
    public boolean rescheduleNextExecution(final Runnable task, long delay, boolean interrupt) {
        SmartRunnable run;
        synchronized (this) {
            if( pool.isShutdown() ) {
                log.error( "Attempt to reschedule task on shutdown scheduler", 
                        new RuntimeException("Attempt to reschedule task on shutdown scheduler") );
                return false;
            }
            
            run = smartRunners.get(task);
            if (run == null) {
                return false;
            }

            run.task.cancel(interrupt);
            run.task = pool.scheduleWithFixedDelay(run, delay, run.period, TimeUnit.MILLISECONDS);
        }
        
        return true;
    } 
    
    public int numRemainingTasks() {
        return smartRunners.size();
    }
    
    public boolean isShutdown() {
        return pool.isShutdown();
    }
    
    public void shutdown() {
        if( this == defaultInstance.GLOBAL.scheduler ) throw new IllegalStateException( "The global scheduler cannot be shutdown.");
        
        synchronized (this) {
            if( requireCleanShutdown && !smartRunners.isEmpty() ) {
                throw new IllegalStateException("There are still " + smartRunners.size() + " tasks scheduled" );
            }
            
            pool.shutdown();
            
            if( !smartRunners.isEmpty() ) {
                StringBuilder message = new StringBuilder( "Scheduled tasks left in scheduler on shutdown: " );
                
                Iterator<SmartRunnable> itr = smartRunners.values().iterator();
                while( itr.hasNext() ) {
                    message.append( itr.next().name );
                    if( itr.hasNext() ) message.append( ", " );
                }
                
                log.info( message );
                
                smartRunners.clear();
            }
        }
    }
    
    @Override
    protected void finalize() {
        if( !pool.isShutdown() ) {
            log.info( "Leaked scheduler " + inactiveName + " not shutdown.");
            
            boolean shutdownScheduler = true;
            synchronized (this) {
                if( !smartRunners.isEmpty() ) {
                    // I don't think we can get here as the tasks themselves should have a reference to the scheduler
                    //  and they are referenced from the queue which is referenced by the scheduler threads.
                    shutdownScheduler = false;
                    StringBuilder message = new StringBuilder( "Scheduled tasks left in leaked scheduler: " );
                    
                    Iterator<SmartRunnable> itr = smartRunners.values().iterator();
                    while( itr.hasNext() ) {
                        message.append( itr.next().name );
                        if( itr.hasNext() ) message.append( ", " );
                    }
                    
                    log.info( message );
                    
                    smartRunners.clear();
                }
            }
            
            if( shutdownScheduler ) pool.shutdown();
        }
    }
    
    public static Scheduler getGlobalInstance() {
        return defaultInstance.GLOBAL.scheduler;
    }
}
