package com.amazon.messaging.seqstore.v3.internal.jmx;

import java.io.IOException;

/**
 * Base SeqStoreStats mbean interface
 * 
 * @author stevenso
 *
 */
public interface SeqStoreViewMBean {
    /**
     * Returns the number of messages in the store.
     * @throws IOException
     */
    long getMessageCount() throws IOException;

    /**
     * Returns the total size of messages in the store.
     * @throws IOException
     */
    long getStoredSize() throws IOException;

    /**
     * Returns the number of messages that are scheduled to become available in the future.
     * @throws IOException
     */
    long getDelayedMessageCount() throws IOException;

    /**
     * Return the number of messages available for dequeue right now.
     * @throws IOException
     */
    long getAvailableMessageCount() throws IOException;
    
    /**
     * Returns the age of the oldest message in the store in milliseconds 
     * @throws IOException
     */
    long getOldestMessageLogicalAge() throws IOException;
    
    /**
     * Returns the number of enqueues done to this store since process startup.
     * @throws IOException
     */
    long getEnqueueCount() throws IOException;
    
    /**
     * Returns the number of buckets used to hold messages for this store.
     * @throws IOException
     */
    long getNumberOfBuckets() throws IOException;
}
