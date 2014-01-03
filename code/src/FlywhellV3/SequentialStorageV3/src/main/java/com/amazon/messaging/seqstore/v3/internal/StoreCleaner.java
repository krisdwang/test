package com.amazon.messaging.seqstore.v3.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.messaging.seqstore.v3.StoredCount;
import com.amazon.messaging.seqstore.v3.config.SeqStoreImmutableConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreReaderV3InternalInterface;
import com.amazon.messaging.seqstore.v3.store.Store;
import com.amazon.messaging.seqstore.v3.store.StoreIterator;
import com.amazon.messaging.utils.collections.CollectionUtils;

public class StoreCleaner<InfoType> {
    private static final Log log = LogFactory.getLog(StoreCleaner.class);
    
    private final Store store;
    
    private final SeqStoreReaderManager<InfoType> readerManager;
    
    private volatile SeqStoreImmutableConfig config;

    public StoreCleaner(Store store, SeqStoreReaderManager<InfoType> readerManager, SeqStoreImmutableConfig config) {
        this.store = store;
        this.readerManager = readerManager;
        this.config = config;
    }
    
    public void setConfig(SeqStoreImmutableConfig config) {
        this.config = config;
    }
    
    public void cleanup() throws SeqStoreException {
        long maxMessageLifeTime = Long.MAX_VALUE;
        long guaranteedRetentionPeriod = Long.MAX_VALUE;

        // Get a stable view of config
        SeqStoreImmutableConfig localConfig = config;
        if (localConfig.getMaxMessageLifeTime() >= 0) {
            maxMessageLifeTime = localConfig.getMaxMessageLifeTime();
        }

        if (localConfig.getGuaranteedRetentionPeriod() >= 0) {
            guaranteedRetentionPeriod = localConfig.getGuaranteedRetentionPeriod();
        }

        // This shouldn't happen
        if (maxMessageLifeTime < guaranteedRetentionPeriod) {
            log.error("Max message lifetime is less than the guaranteed retention period. "
                    + "Using guaranteed retention period as max message lifetime.");
        }
        
        Collection<SeqStoreReaderV3InternalInterface<InfoType>> readers = readerManager.values();
        
        AckIdV3 deleteLevel = getDeleteLevel(readers, guaranteedRetentionPeriod, maxMessageLifeTime);
        
        // Set the ack levels first in case deleteUpTo fails
        for (SeqStoreReaderV3InternalInterface<?> reader : readers) {
            reader.setAckLevel( deleteLevel );
        }
        
        store.deleteUpTo(deleteLevel);
 
        deleteUnneededBuckets(readers, deleteLevel, guaranteedRetentionPeriod);
        store.closeUnusedBucketStores();
    }
    
    private AckIdV3 getDeleteLevel(
                                   Collection<SeqStoreReaderV3InternalInterface<InfoType>> readers, 
                                   long guaranteedRetentionPeriod,
                                   long maxMessageLifeTime) 
        throws SeqStoreException
    {
        AckIdV3 minEnqueueLevel = store.getMinEnqueueLevel();
        
        long currentTime = store.getCurrentStoreTime();
        AckIdV3 forcedDeleteLevel = 
            AckIdV3.min( new AckIdV3(Math.max(0, currentTime - maxMessageLifeTime), false ), minEnqueueLevel );
        
        AckIdV3 guaranteedRetentionLevel  = 
            AckIdV3.min( new AckIdV3(Math.max(0, currentTime - guaranteedRetentionPeriod), false ), minEnqueueLevel );
        
        AckIdV3 firstMessage = null;
        AckIdV3 safeDeleteLevel = guaranteedRetentionLevel;
        for (SeqStoreReaderV3InternalInterface<?> reader : readers) {
            AckIdV3 readerLevel = reader.getAckLevel();
            assert readerLevel != null;
            
            if( readerLevel.compareTo( forcedDeleteLevel ) < 0 ) {
                // If we're going to bump the reader forward because its behind forcedDeleteLevel 
                // bump it forward by as much as possible so we don't keep changing the ack level
                // which would cause extra disk writes and messages between nodes.
                if( firstMessage == null ) {
                    firstMessage = store.getFirstStoredId();
                    firstMessage = AckIdV3.min(firstMessage, minEnqueueLevel);
                }
                
                if( forcedDeleteLevel.compareTo( firstMessage ) < 0 ) {
                    forcedDeleteLevel = new AckIdV3( firstMessage, false );
                }
                
                reader.setAckLevel( forcedDeleteLevel );
                safeDeleteLevel = forcedDeleteLevel;
            } else {
                safeDeleteLevel = AckIdV3.min( safeDeleteLevel, readerLevel );
            }
        }
        
        return safeDeleteLevel;
    }

    private void deleteUnneededBuckets(Collection<SeqStoreReaderV3InternalInterface<InfoType>> readers, 
                                       AckIdV3 deleteLevel, long guaranteedRetentionPeriod)
            throws SeqStoreClosedException, SeqStoreException 
    {
        int minBucketsForAggressiveCleanup = config.getMinBucketsToCompress();
        
        if( store.getNumBuckets() < minBucketsForAggressiveCleanup ) return;
        
        AckIdV3 maxCleanupLevel = null;
        // We can't cleanup past the earliest reader
        for( SeqStoreReaderV3InternalInterface<InfoType> reader : readers ) {
            maxCleanupLevel = AckIdV3.min( maxCleanupLevel, reader.getReadLevel() );
        }
        
        // If there are no readers then every message left in the store must be there because 
        //  of the guaranteed retention period and so can't be deleted
        if( maxCleanupLevel == null ) return;
        
        // Don't need to adjust for min enqueue level here as readers can't go above min enqueue level
        AckIdV3 guaranteedRetentionLevel = new AckIdV3(
                Math.max(0, store.getCurrentStoreTime() - guaranteedRetentionPeriod), false );
        maxCleanupLevel = AckIdV3.min( guaranteedRetentionLevel, maxCleanupLevel );
        
        StoreIterator itr = store.getIterAt( maxCleanupLevel );
        try {
            StoredCount count = store.getCountBefore(itr);
            
            // Not enough buckets on disk
            if( count.getBucketCount() < minBucketsForAggressiveCleanup ) {
                return;
            }
            
            long maxInflight = 0;
            for( SeqStoreReaderV3InternalInterface<InfoType> reader : readers ) {
                maxInflight = Math.max( reader.getInflightMessageCount(), maxInflight );
            }
            
            // This is the maximum number of messages that could end up being deleted. This
            // number can be lower than it should be as the inflights may include
            // many messages above maxCleanupLevel. For the purposes of a rough 
            // estimate it should still be fine and getting the exact number probably
            // isn't worth the overhead.
            long maxPossibleNumDeleteMessages = count.getEntryCount() - maxInflight;
            
            long averageMessagesPerBucket = count.getEntryCount() / count.getBucketCount();
            
            // If the odds that there are at least minBucketsForAggressiveCleanup buckets
            //  available for deletion are too low then just return avoid doing any more  
            //  work. This check can avoid a lot of work for cases when there is a large 
            //  difference between the read and the ack level because the user is acking
            //  messages in batches.
            if( maxPossibleNumDeleteMessages < minBucketsForAggressiveCleanup * averageMessagesPerBucket ) {
                return;
            }
                
            List<NavigableSet<AckIdV3>> inflightMesssageSets = new ArrayList<NavigableSet<AckIdV3>>(readers.size());
            for( SeqStoreReaderV3InternalInterface<InfoType> reader : readers ) {
                inflightMesssageSets.add( reader.getAllInflightIds() );
            }
            
            NavigableSet<AckIdV3> combinedSet = 
                CollectionUtils.mergeSets( inflightMesssageSets );
            
            store.deleteUnneededBuckets(maxCleanupLevel, combinedSet);
        } finally {
            itr.close();
        }
    }

}
