package com.amazon.messaging.seqstore.v3;

import java.util.Collection;

import javax.annotation.concurrent.Immutable;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * The class provides metrics about messages on disk. Unless otherwise noted it is not guaranteed
 * that all fields in an instance of this class are calculated atomically. If enqueues and dequeues
 * are in progress while the function that returned this SeqStoreMetrics object is running then
 * some enqueues/dequeues may be represented in some of the counts but not in others.
 * 
 * @author stevenso
 *
 */
@Immutable
@ToString(includeFieldNames=true)
@EqualsAndHashCode(callSuper=false)
public class SeqStoreMetrics {
    public final static SeqStoreMetrics EmptyStoreMetrics = 
            new SeqStoreMetrics(
                    StoredCount.EmptyStoredCount,
                    StoredCount.EmptyStoredCount,
                    0,
                    0.0,
                    0.0);

    /**
     * The stored count for all messages in the store.
     */
    private final StoredCount totalMessageCount;
    
    /**
     * The stored count for messages that are scheduled to be delivered in the future. 
     * See {@link #getDelayedMessageCount()}
     */
    private final StoredCount delayedMessageCount;

    private final long oldestMessageLogicalAge;
    
    /**
     * Contains the enqueue rate per SECOND and enqueue throughput per BYTES/SECOND for the specific store. This metric is tracked at the Flywheel level and
     * will be zero at the level of individual stores.
     */
    @Getter
    private final double estimatedEnqueueRate;
    
    @Getter
    private final double estimatedEnqueueThroughput;

    public SeqStoreMetrics(
        StoredCount totalMessageCount, StoredCount delayedMessageCount, long oldestMessageLogicalAge, double estimatedEnqueueRate, double estimatedEnqueueThroughput)
    {
        this.totalMessageCount = totalMessageCount;
        this.delayedMessageCount = delayedMessageCount;
        this.oldestMessageLogicalAge = oldestMessageLogicalAge;
        this.estimatedEnqueueRate = estimatedEnqueueRate;
        this.estimatedEnqueueThroughput = estimatedEnqueueThroughput;
    }

    /**
     * Create a SeqStoreMetrics object that is the sum of multiple SeqStoreMetrics objects
     */
    public SeqStoreMetrics (Collection<SeqStoreMetrics> metrics, double estimatedEnqueueRate, double estimatedEnqueueThroughput) {
        PartialStoredCount totalMessageCounts = new PartialStoredCount();
        PartialStoredCount delayedMessageCounts = new PartialStoredCount();

        long tmpOldestMessageLogicalAge = metrics.isEmpty() ? 0 : Long.MIN_VALUE;

        for( SeqStoreMetrics metric : metrics ) {
            totalMessageCounts.add( metric.getTotalMessageCount() );
            delayedMessageCounts.add( metric.getDelayedMessageCount() );
            tmpOldestMessageLogicalAge = Math.max( tmpOldestMessageLogicalAge, metric.getOldestMessageLogicalAge() );
        }

        totalMessageCount = totalMessageCounts.getStoredCount();
        delayedMessageCount = delayedMessageCounts.getStoredCount();
        oldestMessageLogicalAge = tmpOldestMessageLogicalAge;

        this.estimatedEnqueueRate = estimatedEnqueueRate;
        this.estimatedEnqueueThroughput = estimatedEnqueueThroughput;
    }
    
    /**
     * Returns the stored count for all messages in the store.
     */
    public StoredCount getTotalMessageCount() {
        return totalMessageCount;
    }
    
    public long getAvailableEntryCount() {
        return Math.max( totalMessageCount.getEntryCount() - delayedMessageCount.getEntryCount(), 0 );
    }
    
    /**
     * Returns the logical age of the oldest message read in storage. If
     * there are no messages in the store this function returns 0.
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
     * Return the stored count for messages that are scheduled to be delivered in the future.
     */
    public StoredCount getDelayedMessageCount() {
        return delayedMessageCount;
    }
    
    /**
     * Returns the number of messages that are not yet available for dequeuing
     * because their delivery needs to be delayed by configured Q delay
     */
    public long getNumDelayed() {
        return delayedMessageCount.getEntryCount();
    }
}
