package com.amazon.messaging.seqstore.v3.util;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;


import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketStorageType;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.seqstore.v3.store.BucketStore;


public class OpenBucketStoreTracker<T extends BucketStore> {
    private static class OpenBucketStoreIterator<T extends BucketStore> implements Iterator<T> {
        private final Iterator<Set<T>> mapItr;
        
        private Iterator<T> setItr;
        
        public OpenBucketStoreIterator(Iterator<Set<T>> mapItr) {
            this.mapItr = mapItr;
        }

        @Override
        public boolean hasNext() {
            if( mapItr.hasNext() ) {
                return true;
            }
            
            if( setItr != null ) return setItr.hasNext();
            
            return false;
        }
        
        @Override
        public T next() {
            while( setItr == null || !setItr.hasNext() ) {
                if( !mapItr.hasNext() ) {
                    throw new NoSuchElementException();
                }
                
                Set<T> nextSet = mapItr.next();
                setItr = nextSet.iterator();
            }
            
            return setItr.next();
        }
        
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
    
    private class OpenBucketStoresCollection extends AbstractCollection<T> {
        @Override
        public Iterator<T> iterator() {
            return new OpenBucketStoreIterator<T>(openBucketStoresMap.values().iterator());
        }

        @Override
        public int size() {
            // Not perfect but should be close enough
            return openBucketStoreCount.get();
        }
        
        @Override
        public boolean isEmpty() {
            return openBucketStoresMap.isEmpty();
        }
    }
    
    // A map from store id and bucket id to the set of open bucket stores for that bucket 
    // For thread safety the sets are always replaced instead of being modified
    private final ConcurrentMap<BucketStoreKey, Set<T>> openBucketStoresMap 
         = new ConcurrentHashMap<BucketStoreKey, Set<T>>();
    
    private final AtomicInteger openBucketStoreCount = new AtomicInteger(0);
    
    private final AtomicInteger openDedicatedBucketCount = new AtomicInteger(0);
    
    private final Collection<T> openBucketStoresCollection = new OpenBucketStoresCollection();
    
    /**
     * Add a bucket store to the set of open bucket stores.
     * 
     * @param bucketName the name of the bucket the store is for
     * @param store the store to add
     * @throws IllegalArgumentException if the store has already been added
     */
    public void addBucketStore( StoreId storeId, T store ) 
        throws IllegalArgumentException
    {
        BucketStoreKey key = new BucketStoreKey( storeId, store.getBucketId() );
        // The most likely case is that this is the only BucketStore for the given bucket
        //  so try with that assumption first
        Set<T> existingStores = null;

        // Handle the case where other stores exist
        boolean success;
        do {
            Set<T> stores;
            if( existingStores != null ) {
                if( existingStores.contains( store ) ) {
                    throw new IllegalArgumentException(
                            "A bucket store can only be added to the set of stores once");
                }
                stores = new HashSet<T>(existingStores);
                stores.add( store );
            } else {
                stores = Collections.singleton(store);
            }
                
            if( existingStores == null ) {
                existingStores = openBucketStoresMap.putIfAbsent(key, stores);
                success = existingStores == null;
                if( success && store.getBucketStorageType().isDedicated() ) {
                    openDedicatedBucketCount.incrementAndGet();
                }
            } else {
                success = openBucketStoresMap.replace(key, existingStores, stores);
                if( !success ) {
                    existingStores = openBucketStoresMap.get( key );
                }
            }
        } while( !success );
        openBucketStoreCount.incrementAndGet();
    }
    
    public void removeBucketStore( StoreId storeId, T store ) {
        BucketStoreKey key = new BucketStoreKey( storeId, store.getBucketId() );
        
        boolean success;
        do {
            Set<T> existingStores = openBucketStoresMap.get(key);
            if( existingStores == null ) {
                throw new IllegalArgumentException(
                    "BucketStores can only be closed once and must be closed by the persistence manager that created them");
            }
            
            if( existingStores.size() == 1 ) {
                if( !existingStores.contains( store ) ) {
                    throw new IllegalArgumentException(
                            "BucketStores can only be closed once and must be closed by the persistence manager that created them");
                }
                success = openBucketStoresMap.remove(key, existingStores);
                if( success && store.getBucketStorageType().isDedicated() ) {
                    openDedicatedBucketCount.decrementAndGet();
                }
            } else {
                Set<T> stores = new HashSet<T>(existingStores);
                if( !stores.remove( store ) ) {
                    throw new IllegalArgumentException(
                            "BucketStores can only be closed once and must be closed by the persistence manager that created them");
                }
                success = openBucketStoresMap.replace(key, existingStores, stores);
            }
        } while( !success );
        openBucketStoreCount.decrementAndGet();
    }
    
    public int openBucketStoreCount() {
        return openBucketStoreCount.get();
    }
    
    public int openBucketCount() {
        return openBucketStoresMap.size();
    }
    
    public int openDedicatedBucketCount() {
        return openDedicatedBucketCount.get();
    }
    
    /**
     * Return a read only view of the open bucket stores.
     * @return
     */
    public Collection<T> getOpenBucketStores() {
        return openBucketStoresCollection;
    }
    
    public boolean hasOpenStoreForBucket( StoreId storeId, AckIdV3 bucketId ) {
        return openBucketStoresMap.containsKey( new BucketStoreKey(storeId, bucketId) );
    }
    
    public void clear() {
        openBucketStoresMap.clear();
        openBucketStoreCount.set(0);
    }
}
