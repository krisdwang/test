package com.amazon.messaging.seqstore.v3.internal;



import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import net.jcip.annotations.GuardedBy;

import com.amazon.messaging.annotations.TestOnly;
import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.v3.AckId;
import com.amazon.messaging.seqstore.v3.InflightEntry;
import com.amazon.messaging.seqstore.v3.MessageListener;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreInternalException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreUnrecoverableDatabaseException;



/**
 * This class manages delivery of messages to callbacks. It works by scheduling a task whenever the next message is
 * available and then canceling and scheduling an earlier one if an earlier message becomes available.
 * 
 * Note that canceling tasks doesn't remove them from the queue, they only get removed when they would have executed
 * anyway. If long delays are supported it might be worth calling purge occasionally to keep the queue clean.
 * 
 * @author stevenso
 * 
 * @param <IdType>
 * @param <InfoType>
 */
public class MessageListenerManager<IdType extends AckId, InfoType> {

    private static final Log log = LogFactory.getLog (MessageListenerManager.class);
    
    private static boolean catchAssertionErrors = true;

    @TestOnly
    public static void setCatchAssertionErrors(boolean cacheAssertionErrors) {
        MessageListenerManager.catchAssertionErrors = cacheAssertionErrors;
    }

    // The maximum number of messages dispatched in one call. This avoids one store
    // starving the other stores
    //
    public static final int MaxMessagesPerCall = 10;
    
    // How long to wait between retrying calls when getting exceptions from the message source
    public static final int RetryOnExceptionDelayMillis = 100;

    public interface MessageSource<IdType extends AckId, InfoType> {
        /**
         * Get the name to use for logging
         * @return
         */
        public String getName();

        /**
         * Returns the wall-clock time of the next available message or {@link Long#MAX_VALUE} if there are no more
         * messages available.
         * 
         * @throws SeqStoreException If the store is closed or inaccessible.
         */
        public long getTimeOfNextMessage () throws SeqStoreException;

        public InflightEntry<IdType, InfoType> dequeue () throws SeqStoreException;
    }

    private final class DequeueTask implements Runnable {

        private final Lock runningLock = new ReentrantLock ();

        private void notifyException (SeqStoreException e) {
            try {
                listener.onException (e);
            } catch (RuntimeException e2) {
                log.warn ("Exception from callback processing of exception " + e, e2);
            } catch (Throwable e2) {
                log.error ("Error from callback processing of exception " + e, e2);
                if( e2 instanceof Error && !( e2 instanceof AssertionError && catchAssertionErrors ) ) {
                    throw (Error) e2;
                    
                }
            }
        }

        private void notifyMessage (InflightEntry<IdType, InfoType> entry) {
            try {
                listener.onMessage (entry);
            } catch (RuntimeException e) {
                log.warn ("Exception from callback processing of " + entry, e);
            } catch (Throwable e) {
                log.error ("Error from callback processing of " + entry, e);
                if( e instanceof Error && !( e instanceof AssertionError && catchAssertionErrors ) ) {
                    throw (Error) e;
                }
            }
        }

        /**
         * If the task has been shutdown wait till it is known that no more messages will be delivered
         * 
         * @throws IllegalStateException if the listener has not been shut down.
         */
        public void waitTillNoMoreMessagesWillBeDelivered () {
            if (!shutdown) throw new IllegalStateException ();

            // Wait till this thread can get the lock which means no other thread is running the DequeueTask.
            // Note that we get the lock immediately if this is the running thread which is okay
            // as then we know that shutdown will be checked before the next message is delivered
            runningLock.lock ();
            // and then just unlock as knowing that no other thread is running and that shutdown is set
            // is all we need to know
            runningLock.unlock ();
        }

        @Override
        public void run () {

            synchronized (scheduleManagementLock) {
                // Ensure that even if another execution has been scheduled in the mean time, it won't start
                //
                if (nextTaskExecution != null) {
                    nextTaskExecution.cancel (false);
                    nextTaskExecution = null;
                }
                
                // nextScheduledWakeupTime = -1 serves as a reservation mechanism to ensure that in (almost) all cases
                // only one thread is scheduled for dequeuing messages from this store. Once the currently running
                // dequeuer thread has completed, it sets nextScheduledWakeupTime to MAX_VALUE to keep the invariant of
                // nextScheduledWakeupTime always being equal to the earliest message time.
                //
                if (nextScheduledWakeupTime == -1) {
                    log.debug ("Exiting because another task is already running.");
                    return;
                } else if( shutdown ) {
                    // Never going to wakeup again
                    nextScheduledWakeupTime = Long.MAX_VALUE;
                    return;
                } else {
                    // This ensures that no other threads will be scheduling execution before this call returns
                    //
                    nextScheduledWakeupTime = -1;
                }
            }

            long timeOfNextWakeup = -1;

            runningLock.lock ();
            try {
                try {
                    for (int i = 0; i < MaxMessagesPerCall && !shutdown; ++i) {
                        InflightEntry<IdType, InfoType> entry;
                        try {
                            entry = messageSource.dequeue ();
                            if( entry == null && log.isTraceEnabled() ) {
                                log.trace( "Got null dequeue for " + messageSource.getName() );
                            }
                        } catch (SeqStoreException e) {
                            log.error ("Exception fetching message: " + e.getMessage (), e);
                            notifyException (e);
                            entry = null;
                        } catch (RuntimeException e) {
                            log.error ("Exception fetching message: " + e.getMessage (), e);
                            notifyException (new SeqStoreInternalException(e));
                            entry = null;
                        }
    
                        if (entry == null) {
                            break;
                        } else if( log.isTraceEnabled() ) {
                            log.trace( "Dispatching message" + entry.getLogId() + " for " + messageSource.getName() );
                        }
                        notifyMessage (entry);
                    }
                } finally {
                    if (!shutdown) {
                        // Because retrieving the time of the next message is done outside of the scheduleManagementLock, it
                        // is possible that an earlier message comes (through the newMessageAvailable call), just after we
                        // have set timeOfNextWakeup below. This will cause us to miss that update, because
                        // nextScheduledWakeupTime = -1 here. So, set it to MAX_VALUE, before releasing the lock, which will
                        // cause one of newMessageAvailable or timeOfNextWakeup to win by the time we get to the finally
                        // block. The downside to this is that in rare occasions two tasks might be scheduled, which is
                        // something we are willing to live with.
                        //
                        synchronized (scheduleManagementLock) {
                            assert (nextScheduledWakeupTime == -1);
    
                            nextScheduledWakeupTime = Long.MAX_VALUE;
                        }
    
                        try {
                            timeOfNextWakeup = messageSource.getTimeOfNextMessage ();
                        } catch (SeqStoreUnrecoverableDatabaseException e) {
                            log.error ("Database error getting next message time", e);
                            // No new messages will ever be available
                            timeOfNextWakeup = Long.MAX_VALUE;
                            notifyException (e);
                        } catch (SeqStoreException e) {
                            log.error ("Error getting next message time", e);
                            notifyException (e);
                        }
                    }
                }
            } catch (RuntimeException e) {
                log.error ("RuntimeException in MessageListenerManager", e);
            } catch (Throwable e ) {
                log.error ("Error in MessageListenerManager", e);
                if( e instanceof Error && !( e instanceof AssertionError && catchAssertionErrors ) ) {
                    throw (Error) e;
                }
            } finally {
                runningLock.unlock ();

                synchronized (scheduleManagementLock) {
                    if (!shutdown) {

                        // If an unchecked exception occurred during message dequeue, or during the retrieval of the
                        // next message time, schedule a retry in RetryOnExceptionDelay milliseconds
                        //
                        if (timeOfNextWakeup == -1) {
                            log.warn( "Got timeOfNextWakeup = -1" );
                            timeOfNextWakeup = clock.getCurrentTime () + RetryOnExceptionDelayMillis;
                        }

                        if (timeOfNextWakeup < nextScheduledWakeupTime) {
                            // Update the nextScheduledWakeup and schedule a new task only if there is a message waiting
                            // at some point in the future and no other thread beat us to it.
                            long sleepTime = Math.max (0, timeOfNextWakeup - clock.getCurrentTime ());
                            
                            // Schedule the new future first in case schedule throws
                            ScheduledFuture<?> newExecution = 
                                    executor.schedule (dequeueTask, sleepTime, TimeUnit.MILLISECONDS);
                            
                            nextScheduledWakeupTime = timeOfNextWakeup;
                            if (nextTaskExecution != null) nextTaskExecution.cancel (false);
                            nextTaskExecution = newExecution;
                            
                            if( log.isTraceEnabled() ) {
                                log.trace( "Scheduling wakeup for " + messageSource.getName() + " " + sleepTime + " ms in the future." );
                            }
                        }
                    }
                }
            }
        }
    }

    private final Clock clock;

    private final MessageSource<IdType, InfoType> messageSource;

    private final MessageListener<IdType, InfoType> listener;

    private final ScheduledExecutorService executor;

    private final DequeueTask dequeueTask;

    private final Object scheduleManagementLock = new Object ();

    private volatile boolean shutdown;

    // Specifies the time when the next dequeue attempt will be done. Value of -1 is special and means that the dequeue
    // task is currently running, which prevents other tasks from being scheduled. Value of Long.MAX_VALUE means that
    // the dequeue task has completed and is trying to retrieve the time of the next available message.
    //
    @GuardedBy ("scheduleManagementLock")
    private long nextScheduledWakeupTime;

    @GuardedBy ("scheduleManagementLock")
    private ScheduledFuture<?> nextTaskExecution;
    
    public MessageListenerManager (Clock clock, MessageSource<IdType, InfoType> messageSource,
            MessageListener<IdType, InfoType> listener, ScheduledExecutorService executor) {

        this.clock = clock;
        this.messageSource = messageSource;
        this.listener = listener;
        this.executor = executor;
        this.dequeueTask = new DequeueTask ();

        nextScheduledWakeupTime = Long.MAX_VALUE;
        nextTaskExecution = null;

        long initialTime = 0;
        try {
            initialTime = messageSource.getTimeOfNextMessage ();
        } catch (SeqStoreException e) {
            log.warn (
                    "Failed getting the time of the next message when constructing a MessageListenerManager. The dequeue thread will wakeup immediately",
                    e);
        }
        newMessageAvailable (initialTime);
    }

    public void newMessageAvailable (long time) {
        synchronized (scheduleManagementLock) {
            if (shutdown) return;

            if ((nextScheduledWakeupTime > clock.getCurrentTime ()) && (time < nextScheduledWakeupTime)) {
                long sleepTime = Math.max (0, time - clock.getCurrentTime ());
                if( log.isTraceEnabled() ) {
                    log.trace( "Adjusting sleep time for " + messageSource.getName() + " to " + sleepTime );
                }
                
                // Schedule the new future first in case schedule throws
                ScheduledFuture<?> newExecution = 
                        executor.schedule (dequeueTask, sleepTime, TimeUnit.MILLISECONDS);
                
                nextScheduledWakeupTime = time;
                if (nextTaskExecution != null) nextTaskExecution.cancel (false);
                nextTaskExecution = newExecution;
            }
        }
    }

    /**
     * Checks whether the listener wrapped by this manager is the one expected.
     */
    public boolean managesListener (MessageListener<IdType, InfoType> expectedListener) {
        return (listener == expectedListener);
    }

    /**
     * Shutdown message delivery. It is guaranteed that no new messages will be delivered after the future returned by
     * this function completes.
     */
    public Future<Void> shutdown () {
        shutdown = true;
        synchronized (scheduleManagementLock) {
            if (nextTaskExecution != null) {
                nextTaskExecution.cancel (false);
                nextTaskExecution = null;
            }
            nextScheduledWakeupTime = Long.MAX_VALUE;
        }

        try {
            listener.shutdown ();
        } catch (Throwable e) {
            log.warn ("Listener " + listener + " threw exception at shutdown.", e);
        }

        // Do the wait on completion of the shutdown on a separate thread in order to avoid deadlocks
        //
        return executor.submit (new Callable<Void> () {
            @Override
            public Void call () throws Exception {
                dequeueTask.waitTillNoMoreMessagesWillBeDelivered ();
                return null;
            }
        });
    }
}
