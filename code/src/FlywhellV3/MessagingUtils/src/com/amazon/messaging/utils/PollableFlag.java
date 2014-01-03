package com.amazon.messaging.utils;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

/**
 * A class that stores a flag provides and provides methods to wait until the
 * flag is set or cleared.
 * <p>
 * All methods that change or wait for the flag are synchronized. If you want to
 * be sure that the flag does not change between your thread getting woken up by
 * a change and your code acting on the new value you should synchronize on this
 * object.
 * 
 * @author stevenso
 */
public class PollableFlag {
    private volatile boolean flag;
    
    /**
     * Construct a PollableFlag that starts cleared.
     */
    public PollableFlag() {
        flag = false;
    }
    
    /**
     * Construct a PollableFlag that starts with the specified state.
     */
    public PollableFlag(boolean state) {
        this.flag = state;
    }
    
    
    /**
     * Wait for the flag to be set
     * 
     * @param wantedState the state wanted
     * @throws InterruptedException if this thread was interrupted
     */
    public synchronized void waitForSet() throws InterruptedException {
        while( !flag ) {
            wait();
        }
    }
    
    /**
     * Wait for the flag to be set
     * 
     * @param wantedState the state wanted
     * @param timeout how long to wait in milliseconds. If <= 0 this function does not wait
     * @return true if the flag is set, false it the timeout expired without the flag being set
     * @throws InterruptedException if this thread was interrupted
     */
    public synchronized boolean waitForSet(long timeout) throws InterruptedException {
        long endTime = MathUtil.addNoOverflow( System.currentTimeMillis(), timeout);
        long remaining = timeout;
        
        while( !flag && remaining > 0 ) {
            wait( remaining );
            remaining = endTime - System.currentTimeMillis();
        }
        
        return flag;
    }
    
    /**
     * Wait for the state to be cleared
     * 
     * @param unwantedState the state not wanted
     * @throws InterruptedException if this thread was is interrupted
     */
    public synchronized void waitForClear() throws InterruptedException {
        while( flag ) {
            wait();
        }
    }
    
    /**
     * Wait for the state to be cleared
     * 
     * @param unwantedState the state not wanted
     * @param timeout how long to wait in milliseconds. If <= 0 this function does not wait
     * @return true if the flag is cleared, false if the flag was not cleared before the timeout expired
     * @throws InterruptedException if this thread was is interrupted
     */
    public synchronized boolean waitForClear(long timeout) throws InterruptedException {
        long endTime = MathUtil.addNoOverflow( System.currentTimeMillis(), timeout);
        long remaining = timeout;
        
        while( flag && remaining > 0 ) {
            wait( remaining );
            remaining = endTime - System.currentTimeMillis();
        }
        
        return !flag;
    }
    
    /**
     * Set the state.
     * 
     * @param state
     */
    public synchronized void set(boolean value) {
        flag = value;
        notifyAll();
    }
    
    public void set() {
        set( true );
    }
    
    public void clear() {
        set( false );
    }
    
    @SuppressWarnings(
            value="UG_SYNC_SET_UNSYNC_GET", 
            justification="flag is volatile so clients will see the latest value.")
    public boolean get() {
        return flag;
    }
}
