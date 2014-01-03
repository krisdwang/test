package com.amazon.messaging.seqstore.v3.store;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.NavigableSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.coral.metrics.Metrics;
import com.amazon.messaging.annotations.TestOnly;
import com.amazon.messaging.seqstore.v3.Entry;
import com.amazon.messaging.seqstore.v3.SeqStoreMetrics;
import com.amazon.messaging.seqstore.v3.SeqStoreReaderMetrics;
import com.amazon.messaging.seqstore.v3.StoredCount;
import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.config.SeqStoreImmutableConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreInternalException;
import com.amazon.messaging.seqstore.v3.internal.AckIdSource;
import com.amazon.messaging.seqstore.v3.internal.AckIdSourceFactory;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.BucketStorageManager;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internal.StoredEntryV3;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;

/**
 * Implementation of the store that abstracts out all the common mechanisms from
 * the actual persistence medium/method.
 * 
 * @author robburke
 */
public class Store {
    private static final Log log = LogFactory.getLog(Store.class);

    private final BucketManager bm_;

    private final LastAvailable lastAvailable_;

    protected final StoreId destinationId_;

    private final AtomicBoolean isClosed_ = new AtomicBoolean(false);

    /**
     * Only for use in unit tests. See Delay Test
     */
    @TestOnly
    protected Store() {
        destinationId_ = new StoreIdImpl("tester");
        bm_ = null;
        lastAvailable_ = null;
    }

    public Store(
            StoreId destinationId, 
            BucketStorageManager bucketStorageManager,
            SeqStoreImmutableConfig config, 
            AckIdSourceFactory ackIdSourceFactory, 
            StoreSpecificClock clock) 
            throws SeqStoreDatabaseException 
    {
        destinationId_ = destinationId;
        bm_ = new BucketManager(destinationId, config, bucketStorageManager);
        lastAvailable_ = new LastAvailable( destinationId, bm_, ackIdSourceFactory, clock );
    }

    public void close() {
        if (!isClosed_.getAndSet(true)) {
            bm_.close();
        }
    }

    /**
     * Deletes messages with ackIds that are less than lastAvailable
     * 
     * @param key
     *            The level you wish to delete up to.
     * @return The number of messages deleted.
     * @throws IllegalArgumentException if key is null or above the min enqueue level
     */
    public int deleteUpTo(AckIdV3 key) throws SeqStoreException {
        if( key == null ) throw new IllegalArgumentException( "key cannot be null" );

        // Always get lastAvailable before min enqueue level or their order could be wrong
        AckIdV3 lastAvailable = updateAvailable();
        AckIdV3 minEnqueueLevel = getMinEnqueueLevel();
        if( key.compareTo( minEnqueueLevel) > 0 ) throw new IllegalArgumentException( "deletion should be capped below minEnqueueLevel" );
        int numDeleted = bm_.deleteUpTo( key, lastAvailable, minEnqueueLevel );
        
        return numDeleted;
    }

    public void enqueue(AckIdV3 ackId, Entry message, boolean cache, Metrics metrics) throws SeqStoreException, SeqStoreClosedException {
        if (isClosed_.get()) {
            throw new SeqStoreClosedException("The store has already been closed");
        }
        
        if( !lastAvailable_.recordNewId(ackId) ) {
            throw new SeqStoreInternalException( "AckId " + ackId + " is not valid for enqueue into " + destinationId_ );
        }
        
        StoredEntryV3 value = new StoredEntryV3(ackId, message);
        bm_.insert(ackId, value, lastAvailable_.getMinEnqueueLevel(), cache, metrics);
    }

    public StoredEntry<AckIdV3> get(AckIdV3 key) throws SeqStoreException {
        return bm_.getMsg(key);
    }

    /**
     * Get a level that represents the lowest available level for messages. All messages below
     * this message are not available and are eligible for deletion. This is no guarantee that
     * any messages exist at or above this level.
     */
    public AckIdV3 getMinAvailableLevel() throws SeqStoreClosedException {
        return bm_.getCleanedTillLevel();
    }

    public StoreIterator getIterAt(AckIdV3 id) throws SeqStoreDatabaseException, SeqStoreClosedException {
        AckIdV3 startPoint = AckIdV3.max(id, getMinAvailableLevel());
        return new StoreIterator(bm_, startPoint, this);
    }
    
    /**
     * Return a BucketJumper starting at id. This is like getIterAt but
     * allows reaching all messages not just those between min available
     * and min enqueue level. Using this on an active store
     * may miss messages above the min enqueue level. 
     */
    public BucketJumper getBucketJumperAt(AckIdV3 id) throws SeqStoreException {
        BucketJumper retval = new BucketJumper(bm_);
        retval.advanceTo(id);
        return retval;
    }
    
    public AckIdV3 getLastAvailableId() {
        return lastAvailable_.getLastAvailableAckId();
    }

    public AckIdV3 updateAvailable() throws SeqStoreDatabaseException, SeqStoreClosedException {
        return lastAvailable_.updateAvailable();
    }

    public long getTimeOfNextMessage(AckIdV3 current) throws SeqStoreException {
        return lastAvailable_.getTimeOfNextMessage(current);
    }

    // testing use only
    @TestOnly
    BucketManager getBM() {
        return bm_;
    }
    
    public StoreId getDestinationId() {
        return destinationId_;
    }
    
    // display the content for debuging
    @Override
    public String toString() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        pw.print(bm_.toString());
        if (lastAvailable_.getLastAvailableAckId() != null) {
            pw.println("LastAvailable:" + lastAvailable_.getLastAvailableAckId().getTime());
        } else {
            pw.println("LastAvailable: null");
        }
        return sw.toString();
    }

    /**
     * Get the count from now to till the end of the store.
     * @return
     * @throws SeqStoreException
     */
    public StoredCount getDelayedCount() throws SeqStoreException {
        updateAvailable();
        return bm_.getCountAfter(lastAvailable_.getPosition());
    }
    
    /**
     * Get the count from the iterator till now.
     * @param iter
     * @return
     * @throws SeqStoreException
     */
    public StoredCount getQueueDepth(StoreIterator iter) throws SeqStoreException {
        updateAvailable();
        return bm_.getCount(iter.getPosition(), lastAvailable_.getPosition());
    }
    
    /**
     * Get the count from the iterator till the end of the store.
     * 
     * @param iter
     * @return
     * @throws SeqStoreException
     */
    public StoredCount getCountAfter(StoreIterator iter) throws SeqStoreException {
        return bm_.getCountAfter(iter.getPosition());
    }
    
    /**
     * Get the count from start of the store till the iterator
     * 
     * @param iter
     * @return
     * @throws SeqStoreException
     */
    public StoredCount getCountBefore(StoreIterator iter) throws SeqStoreException {
        return bm_.getCountBefore(iter.getPosition());
    }
    
    public SeqStoreMetrics getStoreMetrics() throws SeqStoreException {
        updateAvailable();
        
        StoredCount totalCount = bm_.getCountAfter(null);
        StoredCount delayCount =
                bm_.getDelayedMessageCount(lastAvailable_.getPosition(), lastAvailable_.getCurrentStoreTime());
        long oldestMessageLogicalAge;
        if( totalCount.getEntryCount() == 0 ) {
            oldestMessageLogicalAge = 0; 
        } else {
            AckIdV3 firstMessage = AckIdV3.max( bm_.getFirstStoredMessageId(), bm_.getCleanedTillLevel() );
            oldestMessageLogicalAge = lastAvailable_.getCurrentStoreTime() - firstMessage.getTime();
        }
        
        return new SeqStoreMetrics (totalCount, delayCount, oldestMessageLogicalAge, 0.0, 0.0);
    }

    public long getNumBuckets() {
        return bm_.getNumBuckets();
    }
    
    public long getNumDedicatedBuckets() {
        return bm_.getNumDedicatedBuckets();
    }
    
    public long getCurrentStoreTime() {
        return lastAvailable_.getCurrentStoreTime();
    }

    public void enqueueFinished(AckIdV3 ackId) {
        lastAvailable_.enqueueFinished(ackId);
    }

    public AckIdV3 getAckIdForEnqueue(long requestedTime) {
        return lastAvailable_.requestAckId( requestedTime );
    }

    public AckIdV3 getMinEnqueueLevel() {
        return lastAvailable_.getMinEnqueueLevel();
    }

    public boolean isEmpty() throws SeqStoreClosedException, SeqStoreDatabaseException {
        return !lastAvailable_.hasUnfinishedEnqueue() && bm_.isEmpty();
    }
    
    public void deleteUnneededBuckets(AckIdV3 maxDeletable, NavigableSet<AckIdV3> requiredMessages)
        throws SeqStoreDatabaseException, SeqStoreClosedException
    {
        // Always get lastAvailable before min enqueue level or their order could be wrong
        AckIdV3 lastAvailable = updateAvailable();
        AckIdV3 minEnqueueLevel = getMinEnqueueLevel();
        bm_.deleteUnneededBuckets(maxDeletable, requiredMessages, lastAvailable, minEnqueueLevel );
    }
    
    public void closeUnusedBucketStores() 
            throws SeqStoreClosedException, SeqStoreDatabaseException 
    {
        bm_.closeUnusedBucketStores(getMinEnqueueLevel());
    }

    public AckIdSource getAckIdSource() {
        return lastAvailable_.getAckIdSource();
    }
    
    public AckIdV3 getFirstStoredId() 
            throws SeqStoreClosedException, SeqStoreDatabaseException 
    {
        return bm_.getFirstStoredMessageId();
    }

    public SeqStoreReaderMetrics getStoreBacklogMetrics(AckIdV3 ackLevel, StorePosition dequeueLevel) 
            throws SeqStoreException 
    {
        if( ackLevel == null ) throw new IllegalArgumentException( "ackLevel cannot be null" );
        
        StoredCount inflightMessages;
        StoredCount unreadMessages;
        StoredCount delayedMessages;
        StoredCount totalMessages;
        
        StorePosition ackLevelPosition = bm_.getStorePosition( ackLevel );
        
        totalMessages = bm_.getCountAfter(ackLevelPosition);
        
        // Check that the ack level and the dequeue level are in the expected order
        if( ackLevelPosition != null && ( dequeueLevel == null || ackLevelPosition.compareTo( dequeueLevel ) > 0 ) ) {
            // The ack level is after the dequeue level.
            if( bm_.getCount( dequeueLevel, ackLevelPosition).getEntryCount() != 0 ) {
                // and there are messages between the dequeue level and the ack level. That can happen while a snapshot 
                //  that increases the ack level is being merged with the reader while this is run. That should be rare 
                //  or a large number of duplicates will end up being delivered so warn if we see it.
                log.warn( "AckLevel records messages having been acked that could not have been dequeued for " 
                        + bm_.getStoreId() + ": ackLevel=" + ackLevelPosition + ", dequeueLevel=" + dequeueLevel );
            }
            inflightMessages = StoredCount.EmptyStoredCount;
        } else {
            inflightMessages = bm_.getCount( ackLevelPosition, dequeueLevel );
        }
        unreadMessages = bm_.getCountAfter( dequeueLevel );
        
        lastAvailable_.updateAvailable();
        delayedMessages = bm_.getDelayedMessageCount(
                lastAvailable_.getPosition(), lastAvailable_.getCurrentStoreTime() );

        long oldestMessageAge;
        if( totalMessages.getEntryCount() != 0 ) {
            AckIdV3 firstMessageLevel = 
                    AckIdV3.max( 
                            ackLevel,
                            AckIdV3.max( bm_.getCleanedTillLevel(), bm_.getFirstStoredMessageId() ) );
            
            oldestMessageAge = lastAvailable_.getCurrentStoreTime() - firstMessageLevel.getTime();
        } else {
            oldestMessageAge = 0;
        }
            
        return new SeqStoreReaderMetrics(
                totalMessages, inflightMessages, unreadMessages, delayedMessages, oldestMessageAge);
    }
}
