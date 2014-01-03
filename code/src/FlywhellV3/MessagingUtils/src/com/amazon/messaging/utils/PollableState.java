package com.amazon.messaging.utils;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

/**
 * A class that stores a simple state as an enum and provides methods to wait
 * until the state changes to a desired state or changes away from an unwanted
 * state.
 * <p>
 * All methods that change or wait for state are synchronized. If you want to be
 * sure that the state does not change between your thread getting woken up by a
 * state change and your code acting on the state change you should synchronize
 * on this object.
 * 
 * @author stevenso
 * @param <T>
 *            the type of the enum used for the state
 */
public class PollableState<T extends Enum<T>> {
    private volatile T state;
    
    public PollableState() {
        this.state = null;
    }
    
    public PollableState(T state) {
        this.state = state;
    }
    
    /**
     * Wait for the state to become the wantedState.
     * 
     * @param wantedState the state wanted
     * @throws InterruptedException if this thread was interrupted
     */
    public synchronized void waitForState(T wantedState) throws InterruptedException {
        while( state != wantedState ) {
            wait();
        }
    }
    
    /**
     * Wait for the state to become the wantedState.
     * 
     * @param wantedState the state wanted
     * @param timeout how long to wait in milliseconds. If <= 0 this function does not wait
     * @return true if the state reached the wanted state, false if the desired state was not reached
     * @throws InterruptedException if this thread was interrupted
     */
    public synchronized boolean waitForState(T wantedState, long timeout) throws InterruptedException {
        long endTime = MathUtil.addNoOverflow( System.currentTimeMillis(), timeout);
        long remaining = timeout;
        
        while( state != wantedState && remaining > 0 ) {
            wait( remaining );
            remaining = endTime - System.currentTimeMillis();
        }
        
        return state == wantedState;
    }
    
    /**
     * Wait for the state to not be unwantedState
     * 
     * @param unwantedState the state not wanted
     * @throws InterruptedException if this thread was interrupted
     */
    public synchronized void waitForOtherState(T unwantedState) throws InterruptedException {
        while( state == unwantedState ) {
            wait();
        }
    }
    
    /**
     * Wait for the state to not be unwantedState
     * 
     * @param unwantedState the state not wanted
     * @param timeout how long to wait in milliseconds. If <= 0 this function does not wait
     * @return true if the state is not unwantedState, false if the state stayed as unwantedState
     * @throws InterruptedException if this thread was interrupted
     */
    public synchronized boolean waitForOtherState(T unwantedState, long timeout) throws InterruptedException {
        long endTime = MathUtil.addNoOverflow( System.currentTimeMillis(), timeout);
        long remaining = timeout;
        
        while( state == unwantedState && remaining > 0 ) {
            wait( remaining );
            remaining = endTime - System.currentTimeMillis();
        }
        
        return state != unwantedState;
    }
    
    /**
     * Set the state.
     * 
     * @param state
     */
    public synchronized void set(T state) {
        this.state = state;
        notifyAll();
    }
    
    @SuppressWarnings(
            value="UG_SYNC_SET_UNSYNC_GET", 
            justification="state is volatile so clients will see the latest value.")
    public T get() {
        return state;
    }
}
