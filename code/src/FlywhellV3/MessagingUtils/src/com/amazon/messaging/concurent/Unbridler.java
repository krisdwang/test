package com.amazon.messaging.concurent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import com.amazon.messaging.utils.MathUtil;

/**
 * An abstract class designed to allow performing blocking operations that need
 * to be synchronized and can't be performed all of the time. The
 * synchronization is done through the use of a lock to allow the caller to
 * specify a timeout.
 * 
 * @author kaitchuc
 * @param <Result>
 *            The result of the method to be called in a thread safe manner.
 * @param <Arg>
 *            Any argument to the method that is to be called in a thread safe
 *            manner.
 * @param <Except>
 *            Any exceptions thrown in the process.
 */
public abstract class Unbridler<Result, Arg, Except extends Throwable> {

    private final ReentrantLock lock = new ReentrantLock(true);

    private final Object notificationMutex = new Object();

    /**
     * If this method returns true the thread that called it will proceed to
     * call the method. If this returns false it will either block for the
     * waitTime from getWaitTime or return a timeout.
     * 
     * @return True iff the call to the method can proceed.
     * @throws Except
     *             Throw back to the caller.
     */
    protected abstract boolean acquireAvailable() throws Except;

    /**
     * This is the method that is bridled, and needs to be run in only one
     * thread.
     * 
     * @param arg
     *            Any argument needed.
     * @return A result meaningful to the caller. (This should not be null)
     * @throws Except
     *             Throw an error back to the caller.
     */
    protected abstract Result execute(Arg arg) throws Except;

    /**
     * Allowed for overriding. In case the caller should check aquireAvailable
     * before the requested endtime.
     * 
     * @throws Except
     *             Throw back to the caller.
     */
    protected long getWaitTime(long currentTime, long endTime) throws Except {
        return Math.max(0, endTime - currentTime);
    }

    /**
     * This function checks the result of {@link #acquireAvailable()} and if that 
     * is true returns the result of {@link #execute(Object)}. If {@link #acquireAvailable()}
     * does not return true initially it waits for up to <code>timeout</code> ms 
     * rechecking the result of {@link #acquireAvailable()} every time {@link #wakeUpWaitingThread()} 
     * is called. If {@link #acquireAvailable()} never returns true within the timeout 
     * period this function returns null. 
     * <p>
     * The synchronization on this function is designed such that it is guaranteed
     * that only one thread at a time calls execute and that this function will never block 
     * for longer than the timeout before it calls {@link #execute(Object)}. 
     * 
     * @param timeout
     *            How long to wait in ms, if negative the timeout is treated as infinite.
     * @param arg
     *            The arg to be passed to {@link #execute(Object)}
     * @return The result from call, or null in {@link #acquireAvailable()} never returned
     *            true before the timeout expired.
     * @throws InterruptedException
     *             The current thread was interrupted while waiting.
     * @throws Except
     *             An error from the call method or the getWaitTime method
     */
    public Result call(long timeout, Arg arg) throws InterruptedException, Except {
        long currentTime = System.currentTimeMillis();
        long endTime;

        if (timeout < 0)
            endTime = Long.MAX_VALUE;
        else
            endTime = MathUtil.addNoOverflow(currentTime, timeout);

        if (lock.tryLock(Math.max(0, endTime - currentTime), TimeUnit.MILLISECONDS)) {
            try {
                boolean doExecute = false;
                long time = 0;
                synchronized (notificationMutex) {
                    do {
                        doExecute = acquireAvailable();
                        if (!doExecute && timeout != 0 ) {
                            time = getWaitTime(System.currentTimeMillis(), endTime);
                            if (time > 0)
                                notificationMutex.wait(time);
                        }
                    } while (!doExecute && time > 0);
                }

                if (doExecute)
                    return execute(arg);
            } finally {
                lock.unlock();
            }
        }
        return null;
    }

    public void wakeUpWaitingThread() {
        synchronized (notificationMutex) {
            notificationMutex.notify();
        }
    }
}
