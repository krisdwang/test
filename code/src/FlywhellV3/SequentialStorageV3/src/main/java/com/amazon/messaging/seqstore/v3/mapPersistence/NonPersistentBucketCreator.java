package com.amazon.messaging.seqstore.v3.mapPersistence;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.coral.metrics.Metrics;
import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.config.SeqStoreImmutablePersistenceConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreAlreadyCreatedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDeleteInProgressException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketPersistentMetaData;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketStorageType;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.seqstore.v3.store.BucketStore;
import com.amazon.messaging.seqstore.v3.store.StorePersistenceManager;
import com.amazon.messaging.seqstore.v3.util.OpenBucketStoreTracker;
import com.amazon.messaging.seqstore.v3.util.StringToStoreIdSet;

/**
 * Provides a non persistent version of PersistenceManager using the java maps
 * library.
 * 
 * @author robburke
 */
public class NonPersistentBucketCreator<InfoType> implements StorePersistenceManager {
    private static final Log log = LogFactory.getLog(NonPersistentBucketCreator.class);

    public static final SeqStoreImmutablePersistenceConfig DEFAULT_CONFIG = new SeqStoreImmutablePersistenceConfig(
            false, false, new File("./DoesNotExist/"), new File("./DoesNotExist/"), null, 1000, 2, 2, 2, false);
    
    /**
     * Get the bucket name give the storeId and the bucketId for the bucket.
     */
    private static String getBucketName(StoreId storeId, AckIdV3 bucketId) {
        return storeId.getStoreName() + "|" + bucketId.getTime() + "|" + bucketId.getSeq();
    }
    
    /**
     * Get a string that is the common prefix for all buckets in the given store. This
     * string will always be lexicographically before the names of all buckets for the 
     * store.
     */
    private static String getBucketNamePrefix(String storeName) {
        return storeName + "|";
    }

    /**
     * Get a string that will always be sorted after all buckets for the given store
     * but before any buckets for any following stores.
     */
    private static String getBucketNameTerminator(String storeName) {
        return storeName + "}"; // "}" follows "|" alphabetically...
    }
    
    private final Lock storeChangeLock = new ReentrantLock();

    private final ConcurrentNavigableMap<String, Map<String, AckIdV3>> persistedAckLevels = new ConcurrentSkipListMap<String, Map<String, AckIdV3>>();
    
    
    private final Set<String> storeDeletesInProgress = Collections.newSetFromMap(
            new ConcurrentHashMap<String, Boolean>() );

    private final ConcurrentNavigableMap<String, BucketPersistentMetaData> bucketMetadata = 
            new ConcurrentSkipListMap<String, BucketPersistentMetaData>();

    private final ConcurrentNavigableMap<String, ConcurrentNavigableMap<AckIdV3, StoredEntry<AckIdV3>>> buckets = new ConcurrentSkipListMap<String, ConcurrentNavigableMap<AckIdV3, StoredEntry<AckIdV3>>>();

    private final OpenBucketStoreTracker<MapBucketStore> openBucketStoreTracker = new OpenBucketStoreTracker<MapBucketStore>();

    private final SeqStoreImmutablePersistenceConfig config;

    private final AtomicLong bucketSequenceGenerator = new AtomicLong();
    
    private volatile boolean closed = false;

    public NonPersistentBucketCreator() {
        this(DEFAULT_CONFIG);
    }

    public NonPersistentBucketCreator(SeqStoreImmutablePersistenceConfig config) {
        this.config = config;
    }

    @Override
    public SeqStoreImmutablePersistenceConfig getConfig() {
        return config;
    }

    @Override
    public boolean isHealthy() {
        return !closed;
    }

    @Override
    public void prepareForClose() {
        // Nothing to do
    }

    @Override
    public void close() {
        closed = true;
    }
    
    @Override
    public Collection<BucketPersistentMetaData> getBucketMetadataForStore(StoreId storeId)
        throws SeqStoreDatabaseException
    {
        SortedMap<String, BucketPersistentMetaData> recoverInfo = bucketMetadata.subMap(
                getBucketNamePrefix(storeId.getStoreName()),
                getBucketNameTerminator(storeId.getStoreName()));

        return Collections.unmodifiableCollection( 
                new ArrayList<BucketPersistentMetaData>( recoverInfo.values()) );
    }
    
    @Override
    public BucketStore createBucketStore(
        StoreId destinationId, AckIdV3 bucketId, BucketStorageType preferredStorageType)
        throws SeqStoreDatabaseException, IllegalStateException
    {
        String bucketName = getBucketName(destinationId, bucketId);
        
        BucketPersistentMetaData metadata = new BucketPersistentMetaData(
                bucketSequenceGenerator.getAndIncrement(), bucketId, BucketStorageType.DedicatedDatabase, 
                BucketPersistentMetaData.BucketState.ACTIVE);

        if( bucketMetadata.putIfAbsent(bucketName, metadata) != null ) {
            throw new IllegalStateException("Bucket store " + bucketName + " already exists");
        }
        
        ConcurrentNavigableMap<AckIdV3, StoredEntry<AckIdV3>> bucket = buckets.get(bucketName);
        if (bucket == null) {
            ConcurrentNavigableMap<AckIdV3, StoredEntry<AckIdV3>> newBucket = new ConcurrentSkipListMap<AckIdV3, StoredEntry<AckIdV3>>();
            bucket = buckets.putIfAbsent(bucketName, newBucket);
            if (bucket == null) {
                bucket = newBucket;
            }
        }

        MapBucketStore bucketStore = new MapBucketStore(
                bucketId, metadata.getBucketSequenceId(), bucket);
        openBucketStoreTracker.addBucketStore(destinationId, bucketStore);
        return bucketStore;
    }

    @Override
    public BucketStore getBucketStore(
        StoreId destinationId, AckIdV3 bucketId)
        throws SeqStoreDatabaseException
    {
        String bucketName = getBucketName(destinationId, bucketId);
        
        BucketPersistentMetaData metadata = bucketMetadata.get(bucketName);
        if (metadata == null) {
            throw new IllegalStateException("Bucket store " + bucketName + " already exists");
        } else if( metadata.isDeleted() ) {
            throw new IllegalStateException( "Bucket store " + bucketName + " is already deleted" );
        }

        ConcurrentNavigableMap<AckIdV3, StoredEntry<AckIdV3>> bucket = buckets.get(bucketName);
        if (bucket == null) {
            ConcurrentNavigableMap<AckIdV3, StoredEntry<AckIdV3>> newBucket = new ConcurrentSkipListMap<AckIdV3, StoredEntry<AckIdV3>>();
            bucket = buckets.putIfAbsent(bucketName, newBucket);
            if (bucket == null) {
                bucket = newBucket;
            }
        }

        MapBucketStore bucketStore = new MapBucketStore(
                bucketId, metadata.getBucketSequenceId(), bucket);
        openBucketStoreTracker.addBucketStore(destinationId, bucketStore);
        return bucketStore;
    }

    @Override
    public boolean updateBucketMetadata(
        StoreId destinationId, BucketPersistentMetaData metadata)
        throws SeqStoreDatabaseException
    {
        String bucketName = getBucketName(destinationId, metadata.getBucketId());
        BucketPersistentMetaData oldMetadata = bucketMetadata.get(bucketName);

        do {
            if (oldMetadata == null || oldMetadata.isDeleted())
                return false;
        } while (!bucketMetadata.replace(bucketName, oldMetadata, metadata));

        return true;
    }

    @Override
    public void closeBucketStore(StoreId storeId, BucketStore bucketStore) throws SeqStoreDatabaseException {
        MapBucketStore store;
        try {
            store = (MapBucketStore) bucketStore;
        } catch (ClassCastException e) {
            throw new IllegalArgumentException(
                    "BucketStores can only be closed by the persistence manager that created them");
        }

        openBucketStoreTracker.removeBucketStore(storeId, store);
    }
    
    @Override
    public boolean deleteBucketStore(StoreId storeId, AckIdV3 bucketId)
        throws SeqStoreDatabaseException, IllegalStateException
    {
        String bucketName = getBucketName(storeId, bucketId);
        if (openBucketStoreTracker.hasOpenStoreForBucket(storeId, bucketId)) {
            throw new IllegalStateException(bucketName + " is still open");
        }

        buckets.remove(bucketName);
        return bucketMetadata.remove(bucketName) != null;
    }
    
    @Override
    public void deleteStoreWithNoBuckets(StoreId storeId) throws SeqStoreDatabaseException {
        if( hasBuckets( storeId ) ) {
            throw new IllegalStateException(); 
        }
        
        storeChangeLock.lock();
        try {
            persistedAckLevels.remove(storeId.getStoreName());
        } finally {
            storeChangeLock.unlock();
        }
    }
    
    @Override
    public boolean storeDeletionStarted(StoreId storeId) throws SeqStoreDatabaseException {
        storeChangeLock.lock();
        try {
            if( persistedAckLevels.remove(storeId.getStoreName()) == null ) {
                return false;
            }
            
            if( !storeDeletesInProgress.add( storeId.getStoreName() ) ) {
                log.warn( "Store deletion started for store that was in persistedAckLevels but also in storeDeletesInProgress");
            }
            
            return true;
        } finally {
            storeChangeLock.unlock();
        }
    }
    
    @Override
    public void storeDeletionCompleted(StoreId storeId) throws SeqStoreDatabaseException {
        storeChangeLock.lock();
        try {
            if( !storeDeletesInProgress.remove( storeId.getStoreName()) ) {
                log.info( "storeDeletionCompleted called for store that does was already deleted or has been recreated");
                return;
            }
            
            if( hasBuckets(storeId) ) {
                storeDeletesInProgress.add( storeId.getStoreName() );
                throw new IllegalStateException( "Store " + storeId + " still has buckets."); 
            }
            
            
        } finally {
            storeChangeLock.unlock();        
        }
    }
    
    @Override
    public Collection<BucketPersistentMetaData> markAllBucketsForStoreAsPendingDeletion(StoreId storeId)
        throws SeqStoreDatabaseException, IllegalStateException
    {
        SortedMap<String, BucketPersistentMetaData> bucketsForStore = bucketMetadata.subMap(
                getBucketNamePrefix(storeId.getStoreName()),
                getBucketNameTerminator(storeId.getStoreName()));
        
        for( Map.Entry<String, BucketPersistentMetaData> entry : bucketsForStore.entrySet() ) {
            BucketPersistentMetaData oldMetadata = entry.getValue();
            if( !oldMetadata.isDeleted() ) {
                bucketMetadata.put( entry.getKey(), oldMetadata.getDeletedMetadata() );
            }
        }
        
        return new ArrayList<BucketPersistentMetaData>( bucketsForStore.values() );
    }
    
    @Override
    public boolean hasBuckets(StoreId storeId) throws SeqStoreDatabaseException {
        return !bucketMetadata.subMap(
                getBucketNamePrefix(storeId.getStoreName()),
                getBucketNameTerminator(storeId.getStoreName())).isEmpty();
    }


    @Override
    public Set<StoreId> getStoreIds() {
        return new StringToStoreIdSet(persistedAckLevels.keySet());
    }

    @Override
    public boolean containsStore(StoreId store) throws SeqStoreDatabaseException {
        return persistedAckLevels.containsKey(store.getStoreName());
    }

    @Override
    public void printDBStats() throws SeqStoreDatabaseException {
        System.out.println("No persistence.");
    }
    
    @Override
    public void createStore(StoreId storeId, Map<String, AckIdV3> initialAckLevels)
        throws SeqStoreDatabaseException, SeqStoreAlreadyCreatedException, SeqStoreDeleteInProgressException
    {
        storeChangeLock.lock();
        try {
            if( storeDeletesInProgress.contains( storeId.getStoreName() ) ) {
                throw new SeqStoreDeleteInProgressException( "Store " + storeId + " is still being deleted");
            }
            
            storeDeletesInProgress.remove( storeId.getStoreName() );
            
            if( persistedAckLevels.putIfAbsent(storeId.getStoreName(), initialAckLevels) != null ) {
                throw new SeqStoreAlreadyCreatedException( "createStore called for store " + storeId + " that already exists" );
            }
        } finally {
            storeChangeLock.unlock();
        }        
    }
    
    @Override
    public Set<StoreId> getStoreIdsBeingDeleted() throws SeqStoreDatabaseException {
        storeChangeLock.lock();
        try {
            return new HashSet<StoreId>( new StringToStoreIdSet( storeDeletesInProgress ) );
        } finally {
            storeChangeLock.unlock();
        }
    }
    
    @Override
    public void persistReaderLevels(StoreId storeId, Map<String, AckIdV3> ackLevels) {
        Map<String, AckIdV3> safeCopy = Collections.unmodifiableMap(new HashMap<String, AckIdV3>(ackLevels));
        if( persistedAckLevels.replace(storeId.getStoreName(), safeCopy) == null ) {
            throw new IllegalStateException("persistReaderLevels called for store " + storeId + " that does not exist" );
        }
    }
    
    @Override
    public Map<String, AckIdV3> getReaderLevels(StoreId storeId) throws SeqStoreDatabaseException {
        return persistedAckLevels.get(storeId.getStoreName());
    }

    @Override
    public Object getStatisticsMBean() {
        return null;
    }

    @Override
    public Set<StoreId> getStoreIdsForGroup(String group) throws SeqStoreDatabaseException {
        Set<StoreId> storeIdsForGroup = new HashSet<StoreId>();
        String escapedGroup = StoreIdImpl.escapeGroup(group);
        if (persistedAckLevels.containsKey(escapedGroup)) {
            storeIdsForGroup.add(new StoreIdImpl(group, null));
        }

        String groupWithIdStart = escapedGroup + ":";
        Iterator<String> itr = persistedAckLevels.keySet().tailSet(groupWithIdStart, false).iterator();
        while (itr.hasNext()) {
            String storeName = itr.next();
            if (!storeName.startsWith(groupWithIdStart))
                break;
            storeIdsForGroup.add(new StoreIdImpl(storeName));
        }

        return storeIdsForGroup;
    }

    @Override
    public Set<String> getGroups() {
        TreeSet<String> groups = new TreeSet<String>();

        for (String storeName : persistedAckLevels.keySet()) {
            String group = StoreIdImpl.unescapeGroup(StoreIdImpl.getEscapedGroupFromStoreName(storeName));
            groups.add(group);
        }

        return Collections.unmodifiableSet(groups);
    }

    @Override
    public void reportPerformanceMetrics(Metrics metrics) {
        // Nothing to report
    }

    @Override
    public int getNumBuckets() {
        return bucketMetadata.size();
    }

    @Override
    public int getNumOpenBuckets() {
        return openBucketStoreTracker.openBucketCount();
    }
    
    @Override
    public int getNumOpenDedicatedBuckets() {
        return openBucketStoreTracker.openDedicatedBucketCount();
    }

    @Override
    public int getNumDedicatedBuckets() throws SeqStoreDatabaseException {
        return bucketMetadata.size();
    }
    
    @Override
    public Set<BucketStorageType> getSupportedNewBucketTypes() {
        return Collections.unmodifiableSet( EnumSet.of( BucketStorageType.DedicatedDatabase ) );
    }
}
