package com.amazon.messaging.utils;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import lombok.Synchronized;

/**
 * A simple implementation of future that can have its
 * result set from any thread and that is not tied
 * to a specific runnable.
 * 
 * @author stevenso
 *
 * @param <T>
 */
public class SettableFuture<T> implements Future<T> {
    private static enum State {
        WAITING, DONE, CANCELLED
    };
    
    private PollableState<State> state;
    private Thread runningThread;
    private T result;
    private Throwable exception;
    
    /**
     * Create an unset settable future
     */
    public SettableFuture() {
        state = new PollableState<State>(State.WAITING);
    }
    
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        synchronized (state) {
            if( state.get() != State.WAITING ) return false;
            
            if( mayInterruptIfRunning && runningThread != null ) runningThread.interrupt();
            state.set( State.CANCELLED );
        }
                
        afterDone();
        return true;
    }

    @Override
    @Synchronized("state")
    public T get() throws InterruptedException, ExecutionException {
        state.waitForOtherState( State.WAITING );
        return getResult();
    }

    @Override
    @Synchronized("state")
    public T get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException
    {
        if( !state.waitForOtherState(State.WAITING, unit.toMillis( timeout ) ) ) {
            throw new TimeoutException( this.toString() + ": get timed out after " + timeout + " " + unit.toString() );
        }
        
        return getResult();
    }

    private T getResult() throws ExecutionException {
        if( state.get() == State.CANCELLED ) throw new CancellationException();
        if( exception != null ) throw new ExecutionException( exception );
        return result;
    }

    @Override
    @Synchronized("state")
    public boolean isCancelled() {
        return state.get() == State.CANCELLED;
    }

    @Override
    @Synchronized("state")
    public boolean isDone() {
        return state.get() != State.WAITING;
    }
    
    /**
     * Set the result for this future. Any threads waiting on get will be woken up.
     * @param result
     * @throws IllegalStateException if set or setException have already been called
     */
    public void set(T result) throws DuplicateSetException {
        synchronized (state) {
            runningThread = null;
            State currentState = state.get();
            if( currentState == State.CANCELLED ) return; // Do nothing for a cancelled task
            if( currentState == State.DONE ) throw new DuplicateSetException( "SettableFuture was already set.");
            this.result = result;
            state.set( State.DONE );
        }
        afterDone();
    }
    
    /**
     * Set an exception result for this future. Any threads waiting on get will be woken up.
     * @param result
     * @throws DuplicateSetException if set or setException have already been called
     */
    public void setException(Throwable exception) throws DuplicateSetException {
        synchronized (state) {
            runningThread = null;
            State currentState = state.get();
            if( currentState == State.CANCELLED ) return; // Do nothing for a cancelled task
            if( currentState == State.DONE ) throw new DuplicateSetException( "SettableFuture was already set.");
            this.exception = exception;
            state.set( State.DONE );
        }
        afterDone();
    }
    
    /**
     * Set the thread to interrupt if cancel is called with mayInterruptIfRunning set to true.
     * 
     * @param thread thread to interrupt if cancel is called
     */
    @Synchronized("state")
    public void setRunningThread(Thread thread) {
        runningThread = thread;
    }

    /**
     * This function is called immediately after the value of the future is set or the future
     * is canceled. The default implementation of this function does nothing but may be
     * overridden by subclasses.
     * <p>
     * This function runs in the thread that cancels the future or sets its result. It is 
     * not guaranteed that this function runs before any other threads waiting on the result
     * have run.
     */
    protected void afterDone() {}
    
    @Override
    public String toString() {
        String baseString = super.toString();
        if( isDone() ) {
            if( isCancelled() ) return baseString + "{Cancelled}" ;
            else if( exception != null ) return baseString + "{Exception:{" + exception + "}}";
            else return baseString + "{Result{" + result + "}}";
        } else {
            return baseString + "{Pending}";
        }
    }
}
