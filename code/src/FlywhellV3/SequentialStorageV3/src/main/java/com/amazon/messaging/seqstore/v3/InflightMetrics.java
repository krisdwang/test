package com.amazon.messaging.seqstore.v3;

import net.jcip.annotations.Immutable;
import lombok.Data;
import lombok.ToString;

/**
 * A class used to return a snapshot of the inflight metrics for a reader.
 * 
 * @author stevenso
 * @see {@link StoreBacklogMetrics}, {@link SeqStoreReader#getInflightMetrics()}
 */
@Immutable
@Data
@ToString(includeFieldNames=true)
public final class InflightMetrics {
    public static final InflightMetrics ZeroInFlight = new InflightMetrics( 0, 0 );
    
    private final int numInFlight;
    private final int numAvailableForRedelivery;
    
    /**
     * Gets the number of messages currently inflight. A message is considered inflight if it has been dequeued
     * and the latest delivery has not been nacked or timed out.  
     */
    public int getNumInFlight() {
        return numInFlight;
    }
    
    /**
     * Gets the number of messages available for redelivery. A message is considered to be available for
     * redelivery if it has been dequeued and the latest delivery has either been nacked or has timedout.
     */
    public int getNumAvailableForRedelivery() {
        return numAvailableForRedelivery;
    }
}
