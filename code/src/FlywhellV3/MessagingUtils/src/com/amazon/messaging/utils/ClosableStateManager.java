package com.amazon.messaging.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A simpler version of StateManager for the common case of a component with only an open and a closed state.
 * @author stevenso
 *
 */
public class ClosableStateManager<E extends Exception> extends StateManager<ClosableStateManager.State, E> {
    private static final Log log = LogFactory.getLog(ClosableStateManager.class);
    
    public static enum State {
        OPEN, CLOSED
    }
    
    public interface CloseableExceptionGenerator<E extends Exception> 
        extends ExceptionGenerator<State, E>
    {
        @Override
        public E getException( String operation, State state );
    }
    
    public ClosableStateManager( 
        ExceptionGenerator<State, E> exceptionGenerator, boolean fair )
    {
        super( State.OPEN, exceptionGenerator, fair );
    }
    
    /**
     * Lock the state as open. Throws E if the state is not open. 
     */
    public void lockOpen(String operation) throws E {
        boolean done = false;
        boolean interrupted = false;
        while( !done ) {
            try {
                if( !tryLockRequiredState( State.OPEN, operation, 0 ) ) {
                    // The only reason this thread wouldn't be able to get the lock is if another thread has 
                    // called close() but close hasn't completed yet. Throw anyway as there is no need
                    // to wait for it to do so.
                    throw getExceptionGenerator().getException( operation, getState() );
                }
                done = true;
            } catch( InterruptedException e ) {
                log.info( "Interrupted waiting for lock", e);
                interrupted = true;
            }
        }
        
        if( interrupted ) Thread.currentThread().interrupt();
    }
    
    /**
     * Check that the state is open. Use this if it does not matter if the state changes immediately after
     * the check.
     * @throws E if the state is no open.
     */
    public void checkOpen(String operation) throws E {
        State state = getState();
        if( state != State.OPEN ) 
            throw getExceptionGenerator().getException( operation, state );
    }
    
    /**
     * Return if the state is currently open
     * @return true if this manager's state is open
     */
    public boolean isOpen() {
        return getState() == State.OPEN;
    }

    /**
     * Attempt to change the state to closed. This will only fail if close has previously succeeded. This
     * function will block until all threads that have locked the state have unlocked the state. If you 
     * call this from a thread where you have already locked the state this function will deadlock.
     *  
     * @return true if the state changed, false otherwise
     */
    public boolean close() {
        return updateState( State.OPEN, State.CLOSED );
    }
}
