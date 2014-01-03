package com.amazon.messaging.utils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A class use to manage a state variable in a thread safe way
 * 
 * @author stevenso
 *
 * @param <T>
 */
public class StateManager<T extends Enum<T>, E extends Exception> {
    public static interface ExceptionGenerator<T, E extends Exception> {
        public E getException( String operation, T state );
    }
    
    private static class IllegalStateExceptionGenerator<T> 
        implements ExceptionGenerator<T, IllegalStateException> 
    {
        @Override
        public IllegalStateException getException( String operation, T state ) {
            return new IllegalStateException( "IllegalState for operation " + operation + ": " + state );
        }
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static final ExceptionGenerator<?, IllegalStateException> illegalStateExceptionGenerator = 
        new IllegalStateExceptionGenerator();
    
    @SuppressWarnings("unchecked")
    public static <T> ExceptionGenerator<T, IllegalStateException> getIllegalStateExceptionGenerator() {
        return (ExceptionGenerator<T, IllegalStateException>) illegalStateExceptionGenerator;
    }
    
    private final ReadWriteLock stateLock;
    private final ExceptionGenerator<T, E> exceptionGenerator;
    private volatile T state;
    
    /**
     * Create a StateManager. 
     * 
     * @param initialState the initial state of the StateManager
     * @param exceptionGenerator the generator for exceptions thrown when the state does not match the required
     *  state
     * @param fair should the lock between getting/setting the state be fair
     */
    public StateManager( T initialState, ExceptionGenerator<T, E> exceptionGenerator, boolean fair ) {
        state = initialState;
        stateLock = new ReentrantReadWriteLock( fair );
        this.exceptionGenerator = exceptionGenerator; 
    }
    
    /**
     * If state is the required state then lock the state. Otherwise throws
     * <code>E</code> with failureMessage as the message. If this function does not
     * throw then unlockState must be called once the function that depends on the state not changing
     * is done.
     *  
     * @param required the required state
     * @throws E if the state is not the required state
     */
    public void lockRequiredState( T required, String operation ) throws E {
        stateLock.readLock().lock();
        T currentState = state;
        if( currentState != required ) {
            stateLock.readLock().unlock();
            throw exceptionGenerator.getException( operation, currentState );
        }
    }
    
    
    /**
     * If state is the required state then lock the state. Otherwise throws
     * <code>E</code> with failureMessage as the message. If this function does not
     * throw then unlockState must be called once the function that depends on the state not changing
     * is done.
     *  
     * @param required the required state
     * @param failureMessage the message to put in the {@link IllegalStateException} if the state 
     *   does not match the required state
     * @param how long to wait in milliseconds for the lock
     * @throws E if the state is not the required state
     * @throws InterruptedException 
     */
    public boolean tryLockRequiredState( T required, String operation, long timeout ) throws E, InterruptedException {
        if( !stateLock.readLock().tryLock(timeout, TimeUnit.MILLISECONDS) ) return false;
        
        T currentState = state;
        if( state != required ) {
            stateLock.readLock().unlock();
            throw exceptionGenerator.getException( operation, currentState );
        }
        
        return true;
    }
    
    /**
     * Lock the state at whatever state is currently held.
     * @return the current state
     */
    public T lock() {
        stateLock.readLock().lock();
        return state;
    }
    
    
    /**
     * Attempt to get the state lock without blocking. Returns the locked state if 
     * successful, null if the lock could not be acquired in timeout ms.
     * 
     * @return the current state
     * @throws InterruptedException 
     */
    public T tryLock() {
        if( stateLock.readLock().tryLock() ) {
            return state;
        } else {
            return null;
        }
    }
    
    /**
     * Attempt to get the lock for timeout milliseconds. Returns the locked state if successful, 
     * null if the lock could not be acquired in timeout ms.
     * @return the current state
     * @throws InterruptedException 
     */
    public T tryLock(long timeout) throws InterruptedException {
        if( stateLock.readLock().tryLock(timeout, TimeUnit.MILLISECONDS) ) {
            return state;
        } else {
            return null;
        }
    }
    
    /**
     * Unlock the state so it can be modified by other threads
     */
    public void unlock() {
        stateLock.readLock().unlock();
    }
    
    /**
     * Set the state to newValue if it is currently expected. This function will block until
     * there is no lock on the state. Calling this from a thread that already has a lock on the 
     * state will deadlock.
     * 
     * @param expected the expected state
     * @param newValue the new state
     * @return true if the state was changed.
     */
    public boolean updateState( T expected, T newValue ) {
        if( state != expected ) return false;
        
        stateLock.writeLock().lock();
        try {
            if( state != expected ) return false;
            state = newValue;
            return true;
        } finally {
            stateLock.writeLock().unlock();
        }
    }
    
    public T getState() {
        return state;
    }
    
    public ExceptionGenerator<T, E> getExceptionGenerator() {
        return exceptionGenerator;
    }
}
