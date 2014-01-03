package com.amazon.messaging.seqstore.v3;

import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;


public interface MessageListener<IdType extends AckId, InfoType> {
    /**
     * This is called with a dequeued message whenever one is available
     * @param entry
     */
    public void onMessage(InflightEntry<IdType, InfoType> entry);
    
    /**
     * This is called whenever attempting to retrieve a message failed
     * with the exception that it failed with.
     */
    public void onException(SeqStoreException e);
    
    /**
     * This is called to shutdown a message listener. Once this is called the
     * MessageListener should unblock any threads in onMessage and onException
     * so that thread running the message listener can exit.
     * <p>
     * Note that this runs in a different thread to onMessage and there may be more calls 
     * made to {@link #onMessage(InflightEntry)} after this method is called. 
     * MessageListener implementations should handle any such messages without blocking.
     */
    public void shutdown();
}
