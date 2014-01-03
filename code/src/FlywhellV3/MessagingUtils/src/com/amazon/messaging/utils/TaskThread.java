package com.amazon.messaging.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

/**
 * A thread for background tasks. It can be marked as critical in which case it will shutdown the VM
 * if there is an uncaught exception on the thread. Non critical threads just log the exception and exit.
 * If there is an uncaught Throwable that is not an exception both critical and non-critical thread
 * will shutdown the VM.
 * 
 * @author stevenso
 *
 */
public class TaskThread extends Thread {
    private static final Log log = LogFactory.getLog(TaskThread.class);
    
    private static final UncaughtExceptionHandler exitOnUncaught = new UncaughtExceptionHandler () {
        @Override
        @SuppressWarnings(value="DM_EXIT", justification="Fatal error")
        public void uncaughtException(Thread t, Throwable e) {
            log.fatal( t.getName() + " failed! EXITING!", e);
            System.exit(1);
        }
    };
    
    private static final UncaughtExceptionHandler logOnUncaught = new UncaughtExceptionHandler () {
        @SuppressWarnings(value="DM_EXIT", justification="VM should exit on uncaught error.")
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            if( !( e instanceof Exception ) ) {
                log.fatal( t.getName() + " failed with Error! EXITING!", e);
                System.exit(1);
            } else {
                log.error( t.getName() + " failed! Thread exiting.", e);
            }
        }
    };
    
    private void setUncaughtExceptionHandler(boolean critical) {
        if( critical ) {
            setUncaughtExceptionHandler( exitOnUncaught );
        } else {
            setUncaughtExceptionHandler( logOnUncaught );
        }
    }
    
    public TaskThread( Runnable target, String name, boolean critical ) {
        this( target, name, critical, true );
    }
    
    public TaskThread( Runnable target, String name, boolean critical, boolean daemon ) {
        super( target, name );
        setUncaughtExceptionHandler(critical);
        setDaemon( daemon );
    }
    
    public TaskThread( ThreadGroup group, Runnable target, String name, boolean critical ) {
        this( group, target, name, critical, true );
    }

    public TaskThread( ThreadGroup group, Runnable target, String name, boolean critical, boolean daemon ) {
        super( group, target, name );
        setUncaughtExceptionHandler(critical);
        setDaemon( daemon );
    }
    
    public TaskThread( ThreadGroup group, Runnable target, String name, long stackSize, boolean critical ) {
        this( group, target, name, stackSize, critical, true );
    }
    
    public TaskThread( ThreadGroup group, Runnable target, String name, long stackSize, boolean critical, boolean daemon ) {
        super( group, target, name, stackSize );
        setUncaughtExceptionHandler(critical);
        setDaemon( daemon );
    }
    
    
}
