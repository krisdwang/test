package com.amazon.messaging.utils;


import lombok.Getter;

/**
 * A common base class for many tasks that need to run in the background.
 * 
 * @author stevenso
 *
 */
public abstract class BackgroundService {
    @Getter
    private final String name;
    
    @Getter
    private final boolean critical;
    
    @Getter
    private final Thread backgroundThread;
    
    private volatile boolean shouldStop = false;
    
    /**
     * Create a background service with the specified name. If the service is critical
     * then any uncaught exception from run will cause the VM to shutdown. Otherwise 
     * uncaught exceptions will log an error and the service thread will exit
     * 
     * @param name the name of the service
     * @param critical if the service is a critical service
     */
    public BackgroundService( String name, boolean critical ) {
        this.name = name;
        this.critical = critical;
        
        backgroundThread = new TaskThread( 
                new Runnable() {
                    @Override
                    public void run() { BackgroundService.this.run(); }
                }, name, critical );
    }
    
    /**
     * Subclasses should implement this to run their service. The implementation
     * runs in its own thread and should check {@link #shouldStop()} regularly
     * to see if it should shutdown.
     */
    protected abstract void run();
    
    public boolean isRunning() {
        return backgroundThread.isAlive();
    }
    
    /**
     * Start a service. This may only be called once for the service.
     * 
     * @throws IllegalThreadStateException if the service has already been started
     */
    public void start() throws IllegalStateException {
        backgroundThread.start();
    }
    
    /**
     * Stop a service. Blocks until the service has shutdown. This must not be called from
     * within the thread for the service.
     * 
     * @param interrupt should the service thread be interrupted
     * @param timeout how long to wait for the background thread to shutdown
     * @throws InterruptedException  if this thread is interrupted while waiting for the task to stop
     * @throws IllegalStateException if called from the background thread.
     */
    public void stop(boolean interrupt) throws InterruptedException {
        requestStop(interrupt);
        waitForShutdown();
    }
    
    
    /**
     * Stop a service. Blocks until the service has shutdown or timeout ms have passed. This must not be called from
     * within the thread for the service.
     * 
     * @param timeout how long to wait for the background thread to shutdown
     * @param interrupt should the service thread be interrupted
     * @return true if the background thread stopped in less than timeout milliseconds
     * @throws InterruptedException  if this thread is interrupted while waiting for the task to stop
     * @throws IllegalStateException if called from the background thread.
     */
    public boolean stop(long timeout, boolean interrupt) throws InterruptedException {
        requestStop(interrupt);
        return waitForShutdown(timeout);
    }
    
    /**
     * Request that the background service stop but does not wait for it to finish. 
     * 
     * @param interrupt if true the background thread is interrupted.
     */
    public void requestStop(boolean interrupt) {
        shouldStop = true;
        if( interrupt ) backgroundThread.interrupt();
    }
    
    /**
     * Waits for a service that has been stopped to shutdown. If start has not yet been
     * called returns immediately. This will throw IllegalStateException if called
     * from the background thread.
     * 
     * @throws InterruptedException the thread is interrupted while waiting for the task to stop
     * @throws IllegalStateException if called from the background thread.
     */
    public void waitForShutdown() throws InterruptedException {
        if( isCurrentThreadServiceThread() ) throw new IllegalStateException();
        backgroundThread.join();
    }
    
    /**
     * Waits for a service that has been stopped to shutdown. If start has not yet been
     * called returns immediately. This will throw IllegalStateException if called
     * from the background thread.
     * 
     * @param millis how long to wait for shutdown to complete in millis
     * @return true if the background thread s not running, false otherwise
     * @throws InterruptedException the thread is interrupted while waiting for the task to stop
     * @throws IllegalStateException if called from the background thread.
     */
    public boolean waitForShutdown(long millis) throws InterruptedException {
        if( isCurrentThreadServiceThread() ) throw new IllegalStateException();
        backgroundThread.join(millis);
        return !backgroundThread.isAlive();
    }
    
    /**
     * Returns true if stop has been called. The task may still be running in the background thread even
     * if this returns true if it has not yet noticed that it should stop. 
     */
    public boolean shouldStop() {
        return shouldStop;
    }
    
    /**
     * Returns true of the current thread is the service thread
     * @return
     */
    public boolean isCurrentThreadServiceThread() {
        return Thread.currentThread() == backgroundThread;
    }
}
