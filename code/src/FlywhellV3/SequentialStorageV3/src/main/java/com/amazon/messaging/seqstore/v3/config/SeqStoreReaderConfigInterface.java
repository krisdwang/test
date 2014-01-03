package com.amazon.messaging.seqstore.v3.config;

import com.amazon.messaging.seqstore.v3.InflightInfoFactory;

public interface SeqStoreReaderConfigInterface<InfoType> {

    /**
     * Get how far back in time in milliseconds a reader should start from. Any
     * messages older than the value of this config will be skipped by this
     * reader when it is created/loaded. Messages newer than this will be delivered if
     * they have not already been delivered. If the value is less than 0 or is greater than 
     * the maxMessageLifetime for the store it will be treated as if it is  
     * the maxMessageLifetime. If both this and maxMessageLifeTime are less than 0 then 
     * all messages in the store will be delivered if they have not already been
     * delivered. The default is -1.
     * 
     * @return the max amount of time in milliseconds in the past the reader can start
     *  from.
     */
    public long getStartingMaxMessageLifetime();

    /**
     * Get the in-flight info factory used to create/update in-flight info and
     * set the re-delivery timeout on every dequeue from this reader.
     * 
     * @return InflightInfoFactory to be used by this reader.
     */
    public InflightInfoFactory<InfoType> getInflightInfoFactory();
    
    /**
     * Get the maximum number of messages the in-flight table of this reader can have at once.
     * This number limits the number of unacked messages you can have at a given time. When we 
     * hit this limit, we don't add any more messages to in-flight, until space becomes available.
     * @return configured maximum limit for in-flight table message count
     */
    public int getMaxInflightTableMessageCountLimit();

}