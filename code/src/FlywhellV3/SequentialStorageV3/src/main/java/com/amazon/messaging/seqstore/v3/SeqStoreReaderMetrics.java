package com.amazon.messaging.seqstore.v3;

import java.util.Collection;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@EqualsAndHashCode @ToString
public class SeqStoreReaderMetrics {
    public final static SeqStoreReaderMetrics EmptyStoreMetrics = 
            new SeqStoreReaderMetrics(
                    StoredCount.EmptyStoredCount,
                    StoredCount.EmptyStoredCount,
                    StoredCount.EmptyStoredCount,
                    StoredCount.EmptyStoredCount,
                    0 );

    /**
     * The stored count for all messages in the store above the ack level
     */
    private final StoredCount totalMessageCount;
    
    /**
     * The stored count for messages covered by inflight. See {@link #getInflightMessageCount()}
     */
    private final StoredCount inflightMessageCount;
    
    /**
     * The stored count for messages from the dequeue pointer to the end of the store. See {@link #getUnreadMessageCount()}
     */
    private final StoredCount unreadMessageCount;

    /**
     * The stored count for messages that are scheduled to be delivered in the future. See {@link #getDelayedMessageCount()}
     */
    private final StoredCount delayedMessageCount;
    
    /**
     * Stores the estmated rate of dequeue per SECOND for this specific store. This metric is tracked at the Flywheel
     * level and will be zero at the level of individual stores.
     */
    @Getter
    private final double estimatedDequeueRate;
    
    private final long oldestMessageLogicalAge;

    public SeqStoreReaderMetrics(
        StoredCount totalMessageCount, StoredCount inflightMessageCount, StoredCount unreadMessageCount, 
        StoredCount delayedMessageCount, long oldestMessageLogicalAge)
    {
        this.totalMessageCount = totalMessageCount;
        this.inflightMessageCount = inflightMessageCount;
        this.unreadMessageCount = unreadMessageCount;
        this.delayedMessageCount = delayedMessageCount;
        this.oldestMessageLogicalAge = oldestMessageLogicalAge;
        this.estimatedDequeueRate = 0;
    }
    
    /**
     * Create a SeqStoreMetrics object that is the sum of multiple SeqStoreMetrics objects
     */
    public SeqStoreReaderMetrics (Collection<SeqStoreReaderMetrics> metrics, double estimatedDequeueRate) {
        PartialStoredCount totalMessageCounts = new PartialStoredCount();
        PartialStoredCount inflightMessageCounts = new PartialStoredCount();
        PartialStoredCount unreadMessageCounts = new PartialStoredCount();
        PartialStoredCount delayedMessageCounts = new PartialStoredCount();
        
        long tmpOldestMessageLogicalAge = metrics.isEmpty() ? 0 : Long.MIN_VALUE;
        
        for( SeqStoreReaderMetrics metric : metrics ) {
            totalMessageCounts.add( metric.getTotalMessageCount() );
            inflightMessageCounts.add( metric.getInflightMessageCount() );
            unreadMessageCounts.add( metric.getUnreadMessageCount() );
            delayedMessageCounts.add( metric.getDelayedMessageCount() );
            tmpOldestMessageLogicalAge = Math.max( tmpOldestMessageLogicalAge, metric.getOldestMessageLogicalAge() );
        }
        
        totalMessageCount = totalMessageCounts.getStoredCount();
        inflightMessageCount = inflightMessageCounts.getStoredCount();
        unreadMessageCount = unreadMessageCounts.getStoredCount();
        delayedMessageCount = delayedMessageCounts.getStoredCount();
        oldestMessageLogicalAge = tmpOldestMessageLogicalAge;

        this.estimatedDequeueRate = estimatedDequeueRate;
    }
    
    /**
     * Returns the stored count for all messages in the store. If the 
     * @return
     */
    public StoredCount getTotalMessageCount() {
        return totalMessageCount;
    }
    
    /**
     * Returns the logical age of the oldest unacked message
     * <br>
     * Note: Logical age is defined as the difference between the current
     * time and the effective enqueue time of a message. This can 
     * be negative if the message is delayed.
     * 
     * @return the logical age of the next message read available in storage
     */
    public long getOldestMessageLogicalAge() {
        return oldestMessageLogicalAge;
    }
    
    /**
     * Return the stored count for messages covered by inflight. The count will be larger than the number 
     * of messages inflight as it includes messages above the ack level that haven't been deleted yet.
     */
    public StoredCount getInflightMessageCount() {
        return inflightMessageCount;
    }
    
    
    /**
     * Return the stored count for messages not yet read by the reader. This includes
     * messages that are delayed.
     */
    public StoredCount getUnreadMessageCount() {
        return unreadMessageCount;
    }
    
    
    /**
     * Return the stored count for messages that are scheduled to be delivered in the future.
     */
    public StoredCount getDelayedMessageCount() {
        return delayedMessageCount;
    }
    
    /**
     * Returns the number of messages in the queue currently available for
     * dequeue. A depth of zero means there are no messages available for
     * dequeue at the time of this snapshot. This counts only messages
     * that are available but have not been dequeued, it does not count 
     * messages that are delayed or that that have been dequeued but nacked or 
     * timed out. To get that count see {@link InflightMetrics} 
     */
    public long getQueueDepth() {
        return Math.max( unreadMessageCount.getEntryCount() - delayedMessageCount.getEntryCount(), 0 );
    }

    /**
     * Returns the number of messages that are not yet available for dequeuing
     * because their delivery needs to be delayed by configured Q delay
     */
    public long getNumDelayed() {
        return delayedMessageCount.getEntryCount();
    }
}
