package com.amazon.messaging.seqstore.v3;

/**
 * Data that was retreived from a storage.
 * 
 * @author fateev
 */
public abstract class StoredEntry<T extends AckId> extends Entry {

    /**
     * Get transient id that should be used to acknowledge the message through
     * {@link SeqStoreReader#ack(AckId)} call. Value of this id is not
     * guaranteed to be the same every time the same message is returned from
     * {@link SeqStoreReader#dequeue(long)}.
     */
    public abstract T getAckId();

    /**
     * Time entry is available, or enqueued for non-timer queues as returned by
     * {@link System#currentTimeMillis()}
     */
    @Override
    public abstract long getAvailableTime();
    
    /**
     * LogId is used to identify message in log statements related to its
     * processing inside SeqStore.
     */
    @Override
    public abstract String getLogId();

}
