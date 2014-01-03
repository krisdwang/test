package com.amazon.messaging.seqstore.v3.internal;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.measure.unit.NonSI;
import javax.measure.unit.SI;
import javax.measure.unit.Unit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.MetricsFactory;
import com.amazon.coral.metrics.NullMetricsFactory;
import com.amazon.messaging.annotations.TestOnly;
import com.amazon.messaging.seqstore.v3.config.SeqStoreImmutablePersistenceConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreUnrecoverableDatabaseException;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketPersistentMetaData;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketStorageType;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.seqstore.v3.store.Bucket;
import com.amazon.messaging.seqstore.v3.store.BucketImpl;
import com.amazon.messaging.seqstore.v3.store.StorePersistenceManager;
import com.amazon.messaging.seqstore.v3.store.Bucket.BucketState;
import com.amazon.messaging.utils.Scheduler;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class BucketStorageManager {
    private static final Log log = LogFactory.getLog(BucketStorageManager.class);
    
    static final int MAX_DELETE_ATTEMPTS = 3; 
    
    @Data @RequiredArgsConstructor
    private static final class BucketDeletionRequest {
        private final StoreId storeId;
        private final AckIdV3 bucketId;
        private final long count;
        private final long sizeInBytes;
        private final Runnable completionTask;
        private final int attemptNumber;
        
        public BucketDeletionRequest( StoreId storeId, BucketPersistentMetaData metadata, Runnable completionTask ) {
            this.storeId = storeId;
            this.bucketId = metadata.getBucketId();
            this.count = metadata.getEntryCount();
            this.sizeInBytes = metadata.getByteCount();
            this.completionTask = completionTask;
            this.attemptNumber = 0;
        }
        
        public BucketDeletionRequest getNextAttemptRequest() {
             return new BucketDeletionRequest(
                     storeId, bucketId, count, sizeInBytes,
                     completionTask, attemptNumber + 1 );
        }
    }
    
    private static final Comparator<BucketDeletionRequest> newestBucketsFirstComparator = new Comparator<BucketDeletionRequest>() {
        @Override
        public int compare(BucketDeletionRequest key1, BucketDeletionRequest key2) {
            int attemptDiff = key1.getAttemptNumber() - key2.getAttemptNumber();
            if( attemptDiff != 0 ) {
                // Requests with fewer attempts come first
                return attemptDiff;
            }
            
            AckIdV3 bucketId1 = key1.getBucketId();
            AckIdV3 bucketId2 = key2.getBucketId();

            // Delete the newest buckets first
            int bucketIdCompare = -bucketId1.compareTo( bucketId2 ); 
            if( bucketIdCompare != 0 ) {
                return bucketIdCompare;
            }
            
            // Use the storeId to break ties 
            StoreId storeId1 = key1.getStoreId();
            StoreId storeId2 = key1.getStoreId();
            
            int groupNameCompare = storeId1.getGroupName().compareTo(storeId2.getGroupName());
            if( groupNameCompare != 0) {
                return groupNameCompare;
            }
            
            if( storeId1.getId() == null ) {
                if( storeId2.getId() != null ) {
                    return -1;
                } else {
                    return 0;
                }
            } else {
                if( storeId2.getId() == null ) {
                    return 1;
                } else {
                    return storeId1.getId().compareTo(storeId2.getId());
                }
            }
        }
    };
    
    /**
     * A key to put into the bucket queues to wake up the bucket deletion tasks that are blocked so that they
     * can be shut down
     */
    private static final BucketDeletionRequest wakeUpForShutdownRequest = new BucketDeletionRequest( 
            new StoreIdImpl("Wakeup"), AckIdV3.MAXIMUM, 0, 0, null, 0 );
    
    private final PriorityBlockingQueue<BucketDeletionRequest> sharedDBBucketDeleteQueue = 
            new PriorityBlockingQueue<BucketDeletionRequest>( 32, newestBucketsFirstComparator );
    
    private final PriorityBlockingQueue<BucketDeletionRequest> dedicatedDBBucketDeleteQueue = 
            new PriorityBlockingQueue<BucketDeletionRequest>( 32, newestBucketsFirstComparator );
    
    private final Set<Bucket> dirtyBuckets = 
            Collections.newSetFromMap( new ConcurrentHashMap<Bucket, Boolean>() );
    
    private final AtomicInteger numBuckets = new AtomicInteger();
    
    private final AtomicInteger numDedicatedBuckets = new AtomicInteger();
    
    private final Runnable flushDirtyBucketsTask = new Runnable() {
        @Override
        public void run() {
            flushDirtyBuckets();
        }
    };
    
    private final StorePersistenceManager persistenceManager;
    
    private final Scheduler scheduler;
    
    private final MetricsFactory metricsFactory;
    
    private final List<Thread> deletionTaskThreads;
    
    private final boolean readOnly;
    
    private volatile boolean shutdown = false;
    
    // For unit tests
    private boolean catchAssertionErrors = true;
    
    @RequiredArgsConstructor
    private class DeleteBucketTask implements Runnable {
        private final boolean dedicatedBuckets;
        
        @Override
        public void run() {
            PriorityBlockingQueue<BucketDeletionRequest> queue;
            if( dedicatedBuckets ) {
                queue = dedicatedDBBucketDeleteQueue;
            } else {
                queue = sharedDBBucketDeleteQueue;
            }
            
            while( !shutdown ) {
                BucketDeletionRequest request = null;
                try {
                    try {
                        request = queue.take();
                    } catch( InterruptedException e ) {
                        // Interrupting BDB during a write can cause the BDB to be invalidated. This thread does BDB
                        // writes and so is not safe to be interrupted
                        log.error( "Interrupted in delete bucket thread. Interrupts can invalidate the database!", e );
                        continue;
                    }
                    
                    if( request == wakeUpForShutdownRequest ) {
                        assert shutdown;
                        // Put the key back to allow other tasks to shutdown
                        queue.put(request);
                        break;
                    }
                    
                    boolean success = false;
                    long startTime = System.nanoTime();
                    Metrics metrics = metricsFactory.newMetrics();
                    try {
                        metrics.addDate("StartTime", System.currentTimeMillis() );
                        metrics.addProperty("Operation", "DeleteBucket");
                        metrics.addProperty("StoreGroup", request.getStoreId().getGroupName() );
                        metrics.addDate("BucketStartTime", request.getBucketId().getTime() );
                        if( processBucketDeletionRequest(request, dedicatedBuckets) ) {
                            metrics.addCount("MessagesDeleted", request.getCount(), Unit.ONE );
                            metrics.addCount("BytesDeleted", request.getSizeInBytes(), NonSI.BYTE );
                            metrics.addCount("DedicatedBucket", dedicatedBuckets ? 1 : 0, Unit.ONE );
                            metrics.addTime(
                                    "BucketAge", System.currentTimeMillis() - request.getBucketId().getTime(), SI.MILLI( SI.SECOND ) );
                        } else {
                            metrics.addCount("BucketAlreadyDeleted", 1, Unit.ONE);
                        }
                        success = true;
                    } catch( SeqStoreUnrecoverableDatabaseException e ) {
                        log.fatal( "Unrecoverable exception deleting bucket", e );
                        break;
                    } catch( SeqStoreDatabaseException e ) {
                        log.error("Error deleting bucket " + request + ". Rescheduling", e );
                        rescheduleDeletion( queue, request );
                        metrics.addProperty("SeqStoreDatabaseException", e.getMessage());
                    } finally {
                        metrics.addCount("Fault", success ? 0 : 1, Unit.ONE );
                        metrics.addTime("Time", System.nanoTime() - startTime, SI.NANO( SI.SECOND ) );
                        metrics.addDate("EndTime", System.currentTimeMillis() );
                        metrics.close();
                    }
                } catch( Throwable e ) {
                    log.error( "Unexpected exception in bucket deletion thread", e );
                    if( !catchAssertionErrors && e instanceof AssertionError ) throw (AssertionError) e;
                    
                    if( request != null ) {
                        rescheduleDeletion( queue, request );
                    }
                }
            }
        }
        
        private void rescheduleDeletion( PriorityBlockingQueue<BucketDeletionRequest> queue, BucketDeletionRequest request ) {
            if( ( request.getAttemptNumber() + 1) < MAX_DELETE_ATTEMPTS ) {
                queue.put( request.getNextAttemptRequest() );
            } else {
                log.error( "Discarding delete request " + request + " due to repeated failures" );
            }
        }
    }
    
    public BucketStorageManager(
        StorePersistenceManager persistenceManager, Scheduler scheduler, MetricsFactory metricsFactory) 
            throws SeqStoreDatabaseException 
    {
        this( persistenceManager, scheduler, metricsFactory, new ThreadFactoryBuilder().setDaemon(true).build() );
    }
    
    @TestOnly
    public BucketStorageManager(
        StorePersistenceManager persistenceManager, Scheduler scheduler, ThreadFactory threadFactory) 
            throws SeqStoreDatabaseException
    {
        this( persistenceManager, scheduler, null, threadFactory );
    }
    
    public BucketStorageManager(
        StorePersistenceManager persistenceManager, Scheduler scheduler, 
        MetricsFactory metricsFactory, ThreadFactory threadFactory) 
            throws SeqStoreDatabaseException 
    {
        this.persistenceManager = persistenceManager;
        this.numBuckets.set( persistenceManager.getNumBuckets() );
        this.numDedicatedBuckets.set( this.persistenceManager.getNumDedicatedBuckets() );
        this.scheduler = scheduler;
        if( metricsFactory != null ) {
            this.metricsFactory = metricsFactory;
        } else {
            this.metricsFactory = new NullMetricsFactory();
        }
        
        SeqStoreImmutablePersistenceConfig config = persistenceManager.getConfig();
        this.readOnly = config.isOpenReadOnly();
        
        if( !readOnly ) {
            int numThreads = config.getNumDedicatedDBDeletionThreads() + config.getNumSharedDBDeletionThreads();
            this.deletionTaskThreads = Lists.newArrayListWithCapacity(numThreads);
            for( int i = 0; i < config.getNumDedicatedDBDeletionThreads(); i++ ) {
                Thread deletionTaskThread = threadFactory.newThread(new DeleteBucketTask(true));
                deletionTaskThread.setName("DedicatedDBDeletionTask-" + i );
                deletionTaskThreads.add( deletionTaskThread );
                deletionTaskThread.start();
            }
            
            for( int i = 0; i < config.getNumSharedDBDeletionThreads(); i++ ) {
                Thread deletionTaskThread = threadFactory.newThread(new DeleteBucketTask(false));
                deletionTaskThread.setName("SharedDBDeletionTask-" + i );
                deletionTaskThreads.add( deletionTaskThread );
                deletionTaskThread.start();
            }
            
            scheduler.executePeriodically( 
                    "Dirty Bucket Flush Task", flushDirtyBucketsTask, config.getDirtyMetadataFlushPeriod(), false);
        } else {
            this.deletionTaskThreads = Collections.emptyList();
        }
    }
    
    public void shutdown() {
        shutdown = true;
        
        if( !readOnly ) {
            // Clear the queues and schedule the shutdown
            sharedDBBucketDeleteQueue.clear();
            sharedDBBucketDeleteQueue.put(wakeUpForShutdownRequest);
            dedicatedDBBucketDeleteQueue.clear();
            dedicatedDBBucketDeleteQueue.put(wakeUpForShutdownRequest);
            
            scheduler.cancel(flushDirtyBucketsTask, false, true);
            
            // Wait for the delete bucket threads to finish
            long maxWaitTime = TimeUnit.SECONDS.toNanos(5);
            long stopWaitingTime = System.nanoTime() + maxWaitTime;
            threadLoop: for( Thread thread : deletionTaskThreads ) {
                boolean success = false;
                do {
                    long waitTime = stopWaitingTime - System.nanoTime();
                    if( waitTime <= 0 ) {
                        log.warn( "Timedout waiting for deletion threads to shut down.");
                        break;
                    }
                    
                    try {
                        TimeUnit.NANOSECONDS.timedJoin(thread, waitTime);
                        success = true;
                        if( thread.isAlive() ) {
                            log.warn( "Timedout waiting for deletion threads to shut down.");
                            break threadLoop;
                        }
                    } catch (InterruptedException e) {
                        log.error( "Interrupted shutting down. Interrupts can invalidate the database!", e );
                    }
                } while( !success );
            }
            
            flushDirtyBuckets();
        }
    }
    
    public void markBucketAsDirty(Bucket bucket) {
        if( !readOnly ) dirtyBuckets.add( bucket );
    }
    
    public void flushDirtyBuckets() {
        Set<Bucket> stillDirtyBuckets = null;
        
        Iterator<Bucket> itr = dirtyBuckets.iterator();
        try {
            while( itr.hasNext() ) {
                Bucket bucket = itr.next();
                // Remove before persisting the metadata so any changes done while the metadata is being persisted
                // cause the bucket to marked as dirty again.
                itr.remove(); 
                
                boolean success = false;
                try {
                    // Skip deleted buckets. Continue with closed buckets as all the functions used on the bucket
                    //  are safe to use on closed buckets and the last metadata update may not have been flushed at the time 
                    //  the bucket was closed.
                    if( bucket.getBucketState() == BucketState.DELETED ) {
                        continue;
                    }
                    
                    BucketPersistentMetaData metadata = bucket.getMetadata();
                    // A bucket that has never seen a write shouldn't get here but check
                    // just to be safe.
                    if( metadata != null ) {
                        persistenceManager.updateBucketMetadata( bucket.getStoreName(), metadata );
                    }
                    success = true;
                } catch (SeqStoreUnrecoverableDatabaseException e) {
                    log.warn( "Unrecoverable database exception updating bucket metadata. Shutting down metadata updates.", e);
                    scheduler.cancel(flushDirtyBucketsTask, false, false); // Don't call stop - that will deadlock if this is a scheduled call to flushBuckets
                    break;
                } catch (SeqStoreDatabaseException e) {
                    log.warn( "Got error updating bucket metadata for bucket " + bucket.getBucketId() + " in " + bucket.getStoreName() , e);
                } finally {
                    // If the persist failed keep the bucket dirty
                    if( !success ) {
                        // Add the bucket to the stillDirtyBuckets that will be added back to dirtyBuckets as the end.
                        // We can't do that here or we might loop forever on the same bucket
                        if( stillDirtyBuckets == null ) {
                            stillDirtyBuckets = new HashSet<Bucket>();
                        }
                        stillDirtyBuckets.add( bucket );
                    }
                }
            }
        } finally {
            if( stillDirtyBuckets != null ) {
                dirtyBuckets.addAll( stillDirtyBuckets );
            }
        }
    }
    
    private void postBucketDeletionRequest( BucketStorageType type, BucketDeletionRequest request ) {
        if( type.isDedicated() ) {
            dedicatedDBBucketDeleteQueue.add(request);
        } else {
            sharedDBBucketDeleteQueue.add(request);
        }
    }
    
    /**
     * Delete the provided bucket. This function marks the bucket as deleted
     * and schedules the actual deletion of the data to happen in a background
     * task
     * 
     * @throws SeqStoreDatabaseException
     */
    public void deleteBucket( Bucket bucket )  throws SeqStoreDatabaseException {
        bucket.deleted();
        dirtyBuckets.remove(bucket);
        
        // Null if the bucket wasn't persisted
        BucketPersistentMetaData metadata = bucket.getMetadata();
        
        // If the bucket has been persisted then persist its metadata along with the deleted flag to indicate
        // that the bucket can be safely deleted on restart if the process crashes before bucket deletion completes.
        if( metadata != null ) {
            // Create the metadata for the bucket with the bucket marked as deleted
            persistenceManager.updateBucketMetadata( bucket.getStoreName(), metadata );
            
            if( !shutdown ) {
                BucketDeletionRequest request = new BucketDeletionRequest( 
                        bucket.getStoreName(), metadata, null );
                postBucketDeletionRequest( bucket.getBucketStorageType(), request );
            }
        }
    }
    
    private static class AllBucketsDeletedTask implements Runnable {
        /**
         * The task to run once all buckets have been deleted.
         */
        private final Runnable task;
        
        /**
         * The total number of buckets to be deleted.
         */
        private final int totalBuckets;
        
        private final AtomicInteger bucketsDeleted = new AtomicInteger(0);
        
        private AllBucketsDeletedTask(Runnable task, int totalBuckets) {
            this.task = task;
            this.totalBuckets = totalBuckets;
        }

        @Override
        public void run() {
            if( bucketsDeleted.incrementAndGet() == totalBuckets ) {
                task.run();
            }
        }
    }
    
    /**
     * Mark all the provided buckets for the store as pending deletion and schedule their deletion. 
     * Once all of them have been deleted the completionTask will be called. If this 
     * {@link BucketStorageManager} is shutdown before all buckets have been deleted the completionTask 
     * will never be called. The completionTask will be either run in this thread if all buckets are 
     * deleted before it returns, or in the thread that deleted the last bucket.
     * 
     * @param storeId
     * @param bucketMetadataCollection
     * @param completionTask the task to call when all the buckets have been deleted
     * @throws IllegalStateException if any of the buckets for the store are still open
     * @throws SeqStoreDatabaseException 
     */
    public void deleteBucketsForStore( 
        StoreId storeId, Runnable completionTask) throws SeqStoreDatabaseException, IllegalStateException
    {
        if( shutdown ) return;
        
        Collection<BucketPersistentMetaData> bucketsForStore = 
                persistenceManager.markAllBucketsForStoreAsPendingDeletion(storeId);
        
        AllBucketsDeletedTask allBucketsDeletedTask;
        if( completionTask != null ) {
            allBucketsDeletedTask = new AllBucketsDeletedTask(completionTask, bucketsForStore.size()); 
        } else {
            allBucketsDeletedTask = null;
        }
        
        for( BucketPersistentMetaData metadata : bucketsForStore ) {
            if( shutdown ) return;
            
            BucketDeletionRequest request = new BucketDeletionRequest( storeId, metadata, allBucketsDeletedTask );
            postBucketDeletionRequest(metadata.getBucketStorageType(), request);
        }
    }
    
    public Bucket createBucket( StoreId destinationId, AckIdV3 bucketId, BucketStorageType preferredStorageType ) 
            throws SeqStoreDatabaseException 
    {
        Bucket bucket =
                BucketImpl.createNewBucket(destinationId, bucketId, preferredStorageType, persistenceManager);
        
        if( preferredStorageType.isDedicated() ) {
            numDedicatedBuckets.incrementAndGet();
        }
        numBuckets.incrementAndGet();
        
        return bucket;
    }
    
    /**
     * Get the buckets for a store. The returned map is owned by the caller and will
     * not reflect changes. This must be called only once per store to avoid 
     * problems caused by two Buckets using the same BucketStore.
     * 
     * @param storeId
     * @return
     * @throws SeqStoreDatabaseException
     */
    public ConcurrentNavigableMap<AckIdV3, Bucket> getBucketsForStore(StoreId storeId) 
            throws SeqStoreDatabaseException 
    {
        ConcurrentNavigableMap<AckIdV3, Bucket> buckets = new ConcurrentSkipListMap<AckIdV3, Bucket>();
        for( BucketPersistentMetaData metadata : persistenceManager.getBucketMetadataForStore(storeId) ) {
            if( !metadata.isDeleted() ) {
                Bucket bucket = 
                        BucketImpl.restoreBucket(storeId, metadata, persistenceManager);
                buckets.put( bucket.getBucketId(), bucket );
            } else if( !readOnly ) {
                BucketDeletionRequest request = new BucketDeletionRequest( storeId, metadata, null );
                postBucketDeletionRequest( metadata.getBucketStorageType(), request );
            }
        }
        
        return buckets;
    }
    
    public void reportPerformanceMetrics(Metrics metrics) {
        metrics.addCount("Buckets", getNumBuckets(), Unit.ONE);
        metrics.addCount("DedicatedBuckets", getNumDedicatedBuckets(), Unit.ONE);
        metrics.addCount("SharedDBBucketDeleteQueueSize", sharedDBBucketDeleteQueue.size(), Unit.ONE );
        metrics.addCount("DedicatedDBBucketDeleteQueueSize", dedicatedDBBucketDeleteQueue.size(), Unit.ONE );
        metrics.addCount("NumDirtyBuckets", dirtyBuckets.size(), Unit.ONE );
    }
    
    public int getSharedDBBucketDeleteQueueSize() {
        return sharedDBBucketDeleteQueue.size();
    }
    
    public int getDedicatedDBBucketDeleteQueueSize() {
        return dedicatedDBBucketDeleteQueue.size();
    }

    public long getNumBuckets() {
        return numBuckets.get();
    }
    
    public int getNumDedicatedBuckets() {
        return numDedicatedBuckets.get();
    }
    
    private boolean processBucketDeletionRequest(BucketDeletionRequest request, boolean dedicatedBuckets) 
            throws SeqStoreDatabaseException
    {
        boolean deleted = persistenceManager.deleteBucketStore( request.getStoreId(), request.getBucketId() );
        if( deleted ) {
            if( dedicatedBuckets ) {
                numDedicatedBuckets.decrementAndGet();
            }
            numBuckets.decrementAndGet();
        }
        
        if( request.getCompletionTask() != null ) {
            try {
                request.getCompletionTask().run();
            } catch( Throwable e ) {
                log.warn( "Exception from completion task for bucket deletion request " + request );
            }
        }
        
        return deleted;
    }
    
    /**
     * Set if AssertionError should be caught be the deletion threads. If false any AssertionError thrown will be thrown
     * out of the run method of the thread terminating the deletion thread. This can help with testing but would be
     * bad in production.
     * 
     * @param catchAssertionErrors
     */
    @TestOnly
    void setCatchAssertionErrors(boolean catchAssertionErrors) {
        this.catchAssertionErrors = catchAssertionErrors;
    }
    
    /**
     * Wait up to 1 second for all bucket deletions to finish. This function doesn't give a perfect guarantee that the
     * last buckets picked up by the deletion threads have been finished but proving that would require extra locking
     * that would hurt the performance of the normal case
     * 
     * @throws InterruptedException
     * @throws TimeoutException 
     */
    @TestOnly
    public void waitForDeletesToFinish(long maxTime, TimeUnit unit) throws InterruptedException, TimeoutException {
        long maxSleepTime = TimeUnit.MILLISECONDS.toNanos(20);
        long endTime = System.nanoTime() + unit.toNanos(maxTime);
        for(;;) {
            if( getDedicatedDBBucketDeleteQueueSize() + getSharedDBBucketDeleteQueueSize() == 0 ) {
                // Give time for the last delete to complete
                Thread.sleep(50);
                return;
            }
            
            long remaining = endTime - System.nanoTime();
            if( remaining <= 0 ) {
                throw new TimeoutException("Timedout waiting for bucket deletes to finish");
            } else {
                TimeUnit.NANOSECONDS.sleep( Math.min( maxSleepTime, remaining) );
            }
        }
    }
    
    public Set<BucketStorageType> getSupportedBucketTypes() {
        return persistenceManager.getSupportedNewBucketTypes();
    }
}
