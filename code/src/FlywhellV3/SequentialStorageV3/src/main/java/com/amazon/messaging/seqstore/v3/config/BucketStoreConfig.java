package com.amazon.messaging.seqstore.v3.config;

import java.beans.ConstructorProperties;

import lombok.EqualsAndHashCode;
import lombok.Setter;
import lombok.ToString;
import net.jcip.annotations.Immutable;

/**
 * This class provides the configuration used for determining how large and how long a period will
 * be covered by a bucket 
 * 
 * @author stevenso
 *
 */
@Immutable @EqualsAndHashCode @ToString
public class BucketStoreConfig {
    public static final BucketStoreConfig DEFAULT_DEDICATED = 
            new Builder().withMinPeriod(60)
                         .withMaxPeriod(300)
                         .withMaxEnqueueWindow(300)
                         .withMinSize(10*1024) // 10 mb
                         .build();
    
    public static final BucketStoreConfig DEFAULT_SHARED = 
            new Builder().withMinPeriod(60)
                         .withMaxPeriod(300)
                         .withMaxEnqueueWindow(60)
                         .withMinSize(5*1024) // 5 mb
                         .build();
    
    private final int minPeriod;
    private final int maxPeriod;
    private final int maxEnqueueWindow;
    private final int minSize;
    
    @Setter
    public static class Builder {
        private int minPeriod;
        private int maxPeriod;
        private int maxEnqueueWindow;
        private int minSize;
        
        public BucketStoreConfig build() {
            return new BucketStoreConfig(minPeriod, maxPeriod, maxEnqueueWindow, minSize);
        }
        
        public Builder withMinPeriod(int period) {
            this.minPeriod = period;
            return this;
        }
        
        public Builder withMaxPeriod(int period) {
            this.maxPeriod = period;
            return this;
        }
        
        public Builder withMaxEnqueueWindow(int increment) {
            this.maxEnqueueWindow = increment;
            return this;
        }
        
        public Builder withMinSize(int size) {
            this.minSize = size;
            return this;
        }
    }
    
    @ConstructorProperties(value={"minPeriod", "maxPeriod", "maxEnqueueWindow", "minSize"})
    public BucketStoreConfig(int minPeriod, int maxPeriod, int maxEnqueueWindow,
                                                  int minSize)
    {
        if( minPeriod > maxPeriod ) throw new IllegalArgumentException("minPeriod must be <= maxPeriod" );
        if( minPeriod == 0 ) throw new IllegalArgumentException( "minPeriod must be greater than zero" );
        this.minPeriod = minPeriod;
        this.maxPeriod = maxPeriod;
        this.maxEnqueueWindow = maxEnqueueWindow;
        this.minSize = minSize;
    }

    /**
     * The minimum period (in seconds) covered by a bucket. No bucket will cover any shorter 
     * period but buckets may be extended to cover longer periods if they have not reached 
     * the minimum size. When a new bucket is created for a message the bucket is created with a 
     * start time of the maximum of the end of the previous bucket and the largest multiple 
     * of this that is less than the message's time.
     *  
     * @return The minimum period (in seconds) covered by a bucket
     */
    public int getMinPeriod() {
        return minPeriod;
    }
    
    /**
     * The maximum period (in seconds) covered by a bucket. No bucket will ever be extended 
     * to cover a longer period.
     *  
     * @return The maximum period (in seconds) covered by a bucket
     */
    public int getMaxPeriod() {
        return maxPeriod;
    }
    
    /**
     * The maximum amount of time (in seconds) that a bucket can leave open for new enqueues. This
     * only applies when considering if a message outside of the minimum period can be included in 
     * the bucket. If a message is not in the minimum period for the bucket and the buckets size
     * is below the minimum size then if including the message would not result in an enqueue
     * window larger this the message can be added to the bucket.
     * 
     * @return The maximum amount of time (in seconds) that a bucket can leave open for new enqueues.
     */
    public int getMaxEnqueueWindow() {
        return maxEnqueueWindow;
    }
    
    /**
     * The minimum size in kilobytes for a bucket. While a bucket is smaller than this it may
     * be extended to include any new messages that will not extend the buckets enqueue 
     * window beyond the maximum enqueue window.
     * 
     * @return The minimum size in kilobytes for a bucket.
     * @see #getMaxEnqueueWindow()
     */
    public int getMinimumSize() {
        return minSize;
    }
}
