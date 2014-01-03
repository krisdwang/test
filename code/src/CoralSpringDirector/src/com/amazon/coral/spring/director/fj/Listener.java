package com.amazon.coral.spring.director.fj;

import org.springframework.context.ApplicationEvent;


/**
 * Listens to the events generated during the operation of a ForkJoin.
 * 
 * @author Ovidiu Gheorghies
 */
public interface Listener {

    /**
     * Called from the main thread.
     * 
     * @param forkJoin
     * @param event
     */
    void onBegin(ForkJoin forkJoin, ApplicationEvent event);

    /**
     * Called from the main thread.
     * 
     * @param forkJoin
     * @param event
     */
    void onEnd(ForkJoin forkJoin, ApplicationEvent event);

    /**
     * Called from a dedicated timer thread.
     * 
     * @param forkJoin
     * @param event
     * @param timeSinceStart
     */
    void onTick(ForkJoin forkJoin, ApplicationEvent event, long timeSinceStart);

    /**
     * Called from the worker thread, same thread that completed successfully.
     * 
     * @param forkJoin
     * @param event
     * @param target
     */
    void onSuccess(ForkJoin forkJoin, ApplicationEvent event, Object target);

    /**
     * Called from the worker thread, same thread whose event handler threw an exception.
     * 
     * @param forkJoin
     * @param event
     * @param target
     * @param e
     */
    void onException(ForkJoin forkJoin, ApplicationEvent event, Object target, Exception e);
}
