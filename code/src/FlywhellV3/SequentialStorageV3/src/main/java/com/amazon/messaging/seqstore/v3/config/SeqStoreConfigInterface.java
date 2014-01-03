package com.amazon.messaging.seqstore.v3.config;

public interface SeqStoreConfigInterface {
    /**
     * Returns the period in milliseconds between cleanup executions. This controls
     * how soon old data is purged from the store.
     * 
     * @return The cleaner period, in milliseconds
     */
    public long getCleanerPeriod();

    /**
     * Returns the desired number of nodes to store a message on, including the local node. Flywheel 
     * will attempt to make max( desired redundancy, available nodes) copies of 
     * every message enqueued.
     * 
     * @return The desired number of nodes to store a message on
     */
    public int getDesiredRedundancy();

    /**
     * Returns the required number of nodes a message enqueued to this store must be stored 
     * on for the enqueue to succeed. If Flywheel does not believe that it has enough nodes
     * capable of accepting messages to meet the required redundancy then the enqueue will 
     * fail instantly with a ReplicationFailureException.
     * <p>
     * Note that even if a message is stored on the required  number of hosts the enqueue will 
     * still fail if any of the backup attempts for the message failed. For example if the 
     * desired redundancy for a message is three and the required redundancy is two and Flywheel 
     * attempts to backup to two nodes in addition to the local store the enqueue will fail even 
     * if only one of the backups fails.
     * 
     * @return The the required number of nodes a message enqueued to this store must be stored 
     * on for the enqueue to succeed.
     */
    public int getRequiredRedundancy();

    /**
     * Returns the distribution policy for the store.
     * 
     * @see DistributionPolicy
     * @return The distribution policy for the store 
     */
    public DistributionPolicy getDistributionPolicy();

    /**
     * Returns the maximum amount of time, in milliseconds, to retain messages
     * even if there are readers that have not yet consumed these message. Any
     * number < 0 implies an infinite time, i.e. a messages will never be deleted if 
     * there is any reader that has not acknowledged that message. The retention of  
     * messages is enforced by the cleaner so messages older than this may be 
     * temporarily available.
     * 
     * @return The maximum amount of time, in milliseconds, that messages should be retained, 
     *  or < 0 if there is no maximum
     */
    public long getMaxMessageLifeTime();

    /**
     * Returns the minimum amount of time in milliseconds a message must exist before it
     * can be deleted. Messages newer than this are not deleted even if they have been
     * acked by all readers. Any number < 0 implies an infinite time, i.e. all messages 
     * will be kept forever.
     * 
     * @return The minimum amount of time in milliseconds a message must exist before it
     * can be deleted, or < 0 if messages should never be deleted
     */
    public long getGuaranteedRetentionPeriod();
    
    /**
     * The minimum number of buckets between the ack level and the read level that
     * the cleaner estimates might have had all their messages acked before the it does  
     * the analysis to see if they can be safely deleted. The analysis is potentially
     * expensive so it shouldn't be run unless it is likely to have a large effect. 
     * <p>
     * TODO: Rename this setting to better describe what it is used for
     * 
     * @return The minimum number of buckets between the ack level and the 
     * read level that the cleaner estimates might have had all their messages acked 
     * before it does the analysis to see if they can be safely deleted.
     */
    public int getMinBucketsToCompress();
    
    /**
     * The configuration to be used to determine the size and period covered for buckets
     * backed by a dedicated database.
     * 
     * @return The bucket configuration for buckets backed by a dedicated database 
     */
    public BucketStoreConfig getDedicatedDBBucketStoreConfig();
    
    /**
     * The configuration to be used to determine the size and period covered for buckets
     * backed by a shared database.
     * 
     * @return The bucket configuration for buckets backed by a dedicated database 
     */
    public BucketStoreConfig getSharedDBBucketStoreConfig();
    
    /**
     * The maximum size in kb for the SeqStore before changing to use dedicated database
     * buckets instead of shared database buckets. Note that this is in on a per SeqStore  
     * level not a FlywheelStore level so the actual amount of data in the shared database
     * for a FlywheelStore may be several times larger than this. If 0 then dedicated buckets
     * will always be used.
     * 
     * @return The maximum size in kb for the SeqStore before changing to use dedicated 
     * database buckets instead of shared database buckets.
     */
    public int getMaxSizeInKBBeforeUsingDedicatedDBBuckets();
    
    /**
     * Entries that are not expected to be read from the cache within this amount
     * of time should not be cached and should be evicted from the cache when possible
     * 
     * @return the maximum amount of time we want an object to be kept in the cache. 
     */
    public int getMaxEstimatedCacheTimeSeconds();
}