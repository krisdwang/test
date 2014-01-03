package com.amazon.messaging.interfaces;


/**
 * A class to use as a time source possibly separate from the system clock. This is useful for testing
 * and for tracking time on remote peers.
 *  
 * @author stevenso
 *
 */
public interface Clock {
    public long getCurrentTime();
}
