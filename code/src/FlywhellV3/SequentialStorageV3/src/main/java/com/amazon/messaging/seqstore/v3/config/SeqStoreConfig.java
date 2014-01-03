package com.amazon.messaging.seqstore.v3.config;

import java.util.concurrent.TimeUnit;

import com.amazon.messaging.seqstore.v3.exceptions.InvalidConfigException;

import lombok.Data;
import lombok.ToString;

/**
 * Destination specific configuration.
 */
@Data @ToString(includeFieldNames=true) 
public class SeqStoreConfig implements SeqStoreConfigInterface, Cloneable {
    private int desiredRedundancy = 3;

    private int requiredRedundancy = 1; // 1 to be compatible with local

    private DistributionPolicy distributionPolicy = DistributionPolicy.WideAreaDesired;

    private long maxMessageLifeTime = -1;

    private long guaranteedRetentionPeriod = 0;

    private long cleanerPeriod = 10000;

    private BucketStoreConfig dedicatedDBBucketStoreConfig = BucketStoreConfig.DEFAULT_DEDICATED;
    
    private BucketStoreConfig sharedDBBucketStoreConfig = BucketStoreConfig.DEFAULT_SHARED;
    
    private int maxSizeInKBBeforeUsingDedicatedDBBuckets = 50 * 1024;
    
    private int minBucketsToCompress = 5;
    
    private int maxEstimatedCacheTimeSeconds = (int) TimeUnit.MINUTES.toSeconds(15);
    
    private static final SeqStoreImmutableConfig defaultConfig = new SeqStoreConfig().getImmutableConfig();
    
    public static SeqStoreImmutableConfig getDefaultConfig() {
        return defaultConfig;
    }
    
    /**
     * Set the period in the milliseconds between cleanup executions. This controls
     * how soon old data is purged from the store. The default value of 10000
     * should be fine for most cases.
     * 
     * @param period
     */
    public void setCleanerPeriod(long period) {
        cleanerPeriod = period;
    }

    /**
     * Set the maximum amount of time, in milliseconds, to keep messages around
     * even if there are readers that have not yet consumed these message. Any
     * number < 0 implies an infinite time, i.e. no max retention policy.
     * Default is -1. 0 means that you want to delete all messages right way,
     * which does not make sense
     * 
     * @param maxMessageLifeTime
     */
    public void setMaxMessageLifeTime(long maxMessageLifeTime) {
        if (maxMessageLifeTime == 0) {
            throw new IllegalArgumentException("Max retention preiod cant be 0");
        }
        this.maxMessageLifeTime = maxMessageLifeTime;
    }

    /**
     * Set the maximum message time in seconds. 
     * 
     * @see #setMaxMessageLifeTime(long)
     * @param maxMessageLifeTime
     */
    public void setMaxMessageLifeTimeSeconds(int maxMessageLifeTime) {
        setMaxMessageLifeTime(maxMessageLifeTime * 1000L);
    }

    /**
     * Set the minimum amount of time in milliseconds to retain messages when there
     * are no readers on the store or all of the readers have acked the messages.
     * Any number < 0 implies an infinite time, i.e. no min retention policy.
     * Default is 0.
     * 
     * @param guaranteedRetetionPeriod
     */
    public void setGuaranteedRetentionPeriod(long guaranteedRetetionPeriod) {
        this.guaranteedRetentionPeriod = guaranteedRetetionPeriod;
    }

    /**
     * Set the minimum amount of time in second to retain messages when there
     * are no readers on the store or all of the readers have acked the messages.
     * 
     * @see #setGuaranteedRetentionPeriod(long)
     * @param guaranteedRetetionPeriod
     */
    public void setGuaranteedRetentionPeriodSeconds(int guaranteedRetetionPeriod) {
        this.guaranteedRetentionPeriod = guaranteedRetetionPeriod * 1000L;
    }

    /**
     * Set the distribution policy for the store. 
     * Default is {@link DistributionPolicy#WideAreaDesired}
     * 
     * @see DistributionPolicy
     * @param distributionPolicy
     *            not null
     */
    public void setDistributionPolicy(DistributionPolicy distributionPolicy) {
        if (distributionPolicy == null) {
            throw new IllegalArgumentException("Invalid distributionPolicy: null");
        }
        this.distributionPolicy = distributionPolicy;
    }

    /**
     * Set the minimum number of nodes to store a message for enqueue to be successful.
     * Default is 1.
     * 
     * @param requiredRedundancy
     *            Allowed values are 1, 2, and 3.
     */
    public void setRequiredRedundancy(int requiredRedundancy) {
        if ((requiredRedundancy < 1) || (requiredRedundancy > 3)) {
            throw new IllegalArgumentException("Invalid requiredRedundancy=" + requiredRedundancy
                    + ". Valid values are 1, 2 and 3.");
        }
        this.requiredRedundancy = requiredRedundancy;
    }

    /**
     * Set the desired number of nodes to store a message. Default is 3.
     * 
     * @param desiredRedundancy
     *            Allowed values are 1, 2, and 3.
     */
    public void setDesiredRedundancy(int desiredRedundancy) {
        if ((desiredRedundancy < 1) || (desiredRedundancy > 3)) {
            throw new IllegalArgumentException("Invalid desiredRedundancy=" + desiredRedundancy
                    + ". Valid values are 1, 2 and 3.");
        }
        this.desiredRedundancy = desiredRedundancy;
    }
    
    /**
     * Does a sanity check on the various config values of the seq-store config.
     * Specifically we check for: <li>Desired Redundancy >= Required Redundancy
     * <li>Required Redundancy MUST be atleast <code>X</code> for
     * <code>Span<b>X</b>AreasRequired</code> <li>A warning is logged IF,
     * desired redundancy is 1 when distribution policy is WideAreaDesired.
     * 
     * @return null if the config is valid, otherwise a string describing why the config is invalid
     */
    public String validate() {
        // Compare redundancy values, ALWAYS, desired >= required
        if (desiredRedundancy < requiredRedundancy) {
            return "DesiredRedundancy MUST be greater than RequiredRedundancy";
        }

        // See if the redundancy values conform to specified distribution policy
        if ((distributionPolicy == DistributionPolicy.Span2AreasRequired) && (requiredRedundancy < 2)) {
            return "RequiredRedundancy MUST be atleast 2 for distribution policy: "
                    + DistributionPolicy.Span2AreasRequired;
        }

        if ((distributionPolicy == DistributionPolicy.Span3AreasRequired) && (requiredRedundancy < 3)) {
            return "RequiredRedundancy MUST be 3 for distribution policy: "
                    + DistributionPolicy.Span3AreasRequired;
        }

        if (guaranteedRetentionPeriod >= 0) {
            if ((maxMessageLifeTime >= 0) && (maxMessageLifeTime < guaranteedRetentionPeriod)) {
                return "The guaranteed retention period must be longer than the guaranteed retention period.";
            }
        } else if (maxMessageLifeTime >= 0) {
            return "The guaranteed retention period cannot be infinite if the max message lifetime is not infinite.";
        }
        
        if( dedicatedDBBucketStoreConfig == null ) {
            return "The configuration for dedicated database buckets must be provided";
        }
        
        if( sharedDBBucketStoreConfig == null ) {
            return "The configuration for shared database buckets must be provided";
        }
        
        if( maxSizeInKBBeforeUsingDedicatedDBBuckets < 0 ) {
            return "maxSizeInKBBeforeUsingDedicatedDBBuckets must be greater or equal to 0";
        }
        
        return null;
    }
    
    /**
     * Set the configuration to be used to determine the size and period covered for buckets
     * backed by a dedicated database.
     */
    public void setDedicatedDBBucketStoreConfig(BucketStoreConfig config) {
        dedicatedDBBucketStoreConfig = config;
    }
    
    /**
     * Set the configuration to be used to determine the size and period covered for buckets
     * backed by the shared database.
     */
    public void setSharedDBBucketStoreConfig(BucketStoreConfig config) {
        this.sharedDBBucketStoreConfig = config;
    }

    /**
     * Set the minimum number of buckets available that could potential be deleted before
     * SeqStore attempts to delete unused buckets.
     * 
     * @param val
     */
    public void setMinBucketsToCompress(int val) {
        this.minBucketsToCompress = val;
    }
    
    /**
     * Set the maximum size in kb for the SeqStore before changing to use dedicated database
     * buckets instead of shared database buckets. Note that this is in on a per-SeqStore  
     * level not a FlywheelStore level so the actual amount of data in the shared database
     * for a FlywheelStore may be several times larger than this.
     */
    public void setMaxSizeInKBBeforeUsingDedicatedDBBuckets(int sizeInKb) {
        this.maxSizeInKBBeforeUsingDedicatedDBBuckets = sizeInKb;
    }
    
    /**
     * Set the maximum amount of time that the SeqStore aim to keep entries in the cache.
     * Entries not expected to be read within this time should not be cached
     * or should be evicted when possible.
     * 
     * @return the maximum amount of time we want an object to be kept in the cache. 
     */
    public void setMaxEstimatedCacheTimeSeconds(int seconds) {
        this.maxEstimatedCacheTimeSeconds = seconds;
    }
    
    /**
     * Constructs an immutable config object from this object after validating it.
     * 
     * @throws InvalidConfigException if the config is not valid according to {@link #validate()}
     * @return An immutable copy of this config.
     */
    public SeqStoreImmutableConfig getImmutableConfig() throws InvalidConfigException {
        String validResult = validate();
        if( validResult != null ) throw new InvalidConfigException( validResult );
        return new SeqStoreImmutableConfig( 
                desiredRedundancy, requiredRedundancy, distributionPolicy,
                maxMessageLifeTime, guaranteedRetentionPeriod,
                cleanerPeriod, dedicatedDBBucketStoreConfig, 
                sharedDBBucketStoreConfig,
                maxSizeInKBBeforeUsingDedicatedDBBuckets, 
                minBucketsToCompress, maxEstimatedCacheTimeSeconds );
    }
    
    @Override
    public SeqStoreConfig clone() {
        // There is no need to change anything as all fields use immutable values
        try {
            return (SeqStoreConfig) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }
}
