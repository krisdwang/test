package com.amazon.messaging.concurent;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import lombok.Setter;

import edu.umd.cs.findbugs.annotations.CheckForNull;

/**
 * A hash map that allows locking entries to prevent other threads from viewing
 * or modifying them. Before an entry in the map can be modified it must be locked. Threads
 * attempting to get a value from the map while its key is locked will be blocked until 
 * the lock is released. Iterating over the map while changes are in progress is okay
 * but iterators will block on locked keys until the key is unlocked.
 * <p>
 * This map is a useful alternative to ConcurrentHashMap in cases where constructing and 
 * destroying the object is expensive or must be done only once, e.g. managing stores 
 * in SequentialStorage
 * <p>
 * Example Use:
 * <pre>
 * {@code LockingMap<String, Store> storeMap = LockingMap.builder().withFairnessPolicy(false).build();}
 * ...
 * if( !storeMap.contains( storeName ) {
 *     Store store = storeMap.lock( storeName );
 *     try {
 *         if( store == null ) {
 *             store = storeFactory.createStore( storeName );
 *             storeMap.put( storeName, store );
 *         }
 *     } finally {
 *         storeMap.unlock( storeName );
 *     }
 * }
 * </pre>
 * 
 * 
 * @author stevenso
 *
 * @param <K>
 * @param <V>
 */
public class LockingMap<K,V> extends AbstractMap<K, V> {
    public static class Builder {
        private boolean fair = false;
        
        private int initialCapacity = 16;
        
        private float loadFactor = 0.75f;
        
        private int concurrencyLevel = 16;
        
        /**
         * Set the fairness policy (default false)
         */
        public Builder withFairnessPolicy(boolean fair) {
            this.fair = fair;
            return this;
        }
        
        /**
         * Set the load factor (default 0.75)
         */
        public Builder withLoadfactor(float loadFactor) {
            this.loadFactor = loadFactor;
            return this;
        }
        
        /**
         * Set the concurrency factor (default 16)
         */
        public Builder withConcurrencyLevel(int concurrencyLevel) {
            this.concurrencyLevel = concurrencyLevel;
            return this;
        }
        
        public Builder withInitialCapacity(int initialCapacity) {
            this.initialCapacity = initialCapacity;
            return this;
        }
        
        /**
         * Build the map
         */
        public <K,V> LockingMap<K,V> build() {
            return new LockingMap<K, V>( initialCapacity, loadFactor, concurrencyLevel, fair );
        }
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    @SuppressWarnings("serial")
    private static final class LockedValue<V> extends ReentrantLock {
        public LockedValue(V value, boolean fair) {
            this.value = value;
        }
        
        @Setter
        private V value;
        
        @CheckForNull
        public V getValue() {
            return value;
        }
    }
    
    private class EntrySet extends AbstractSet<Map.Entry<K, V>> {
        @Override
        public Iterator<Map.Entry<K, V>> iterator() {
            return new EntrySetIterator( dataMap.entrySet().iterator() );
        }

        @Override
        public int size() {
            return LockingMap.this.size();
        }
    }
    
    private class EntrySetIterator implements Iterator<Map.Entry<K, V>> {
        private final Iterator<Map.Entry<K, Object>> mapItr;
        
        private Map.Entry<K, V> previous;
        
        private Map.Entry<K, V> next;
        
        private EntrySetIterator(Iterator<java.util.Map.Entry<K, Object>> mapItr) {
            super();
            this.mapItr = mapItr;
        }

        @Override
        public boolean hasNext() {
            while( next == null && mapItr.hasNext() ) {
                Map.Entry<K, Object> entry = mapItr.next();
                V value = getRealValue(entry.getKey(), entry.getValue() );
                if( value != null ) {
                    next = new SimpleEntry<K,V>(entry.getKey(), value);
                }
            }
            
            return next != null;
        }

        @Override
        public Map.Entry<K, V> next() {
            if( !hasNext() ) throw new NoSuchElementException();
            previous = next;
            next = null;
            return previous;
        }

        @Override
        public void remove() {
            if( previous == null ) {
                throw new IllegalStateException();
            }
            LockingMap.this.remove(previous.getKey());
            previous = null;
        }
        
    }
    
    private final ConcurrentMap<K,Object> dataMap;
    
    private final boolean fair;
    
    private EntrySet entrySet;
    
    /**
     * Create a new, empty map with a default initial capacity (16), load factor (0.75) and concurrencyLevel (16)
     * and the not fair lock ordering policy 
     */
    public LockingMap() {
        this(false);
    }
    
    /**
     * Create a new, empty map with a default initial capacity (16), load factor (0.75) and concurrencyLevel (16)
     */
    public LockingMap(boolean fair) {
        this.fair = fair;
        this.dataMap = new ConcurrentHashMap<K, Object>();
    }
    
    /**
     * Create a new empty map with the specified configuration
     * @param initialCapacity
     * @param loadFactor
     * @param concurrencyLevel
     * @param fair
     */
    public LockingMap(int initialCapacity, float loadFactor, int concurrencyLevel, boolean fair) {
        this.fair = fair;
        this.dataMap = new ConcurrentHashMap<K, Object>(initialCapacity, loadFactor, concurrencyLevel);
    }
    

    /**
     * Lock the given key. No other operations can be done on the
     * key until the lock is released
     * @param key the key to lock
     * @return the current value for key
     */
    public V lock(K key) {
        while( true ) {
            Object curVal = dataMap.get(key);
            if( curVal instanceof LockedValue ) {
                @SuppressWarnings("unchecked")
                LockedValue<V> lockedValue = ( LockedValue<V> ) curVal;
                
                lockedValue.lock();
                if( dataMap.get(key) == lockedValue ) {
                    return lockedValue.getValue();
                } else {
                    lockedValue.unlock();
                }
            } else {
                @SuppressWarnings("unchecked")
                LockedValue<V> lockedValue =  new LockedValue<V>((V) curVal, fair);
                
                boolean success = false;
                
                lockedValue.lock();
                try {
                    success = doReplace( key, curVal, lockedValue );
                    if( success ) {
                        return lockedValue.getValue();
                    }
                } finally {
                    if( !success ) {
                        // Nothing else should reference this but be safe
                        lockedValue.unlock();
                    }
                }
            }
        }
    }
    
    /**
     * Unlock the specified key allowing other threads to read its current value.
     */
    public void unlock(K key) {
        Object curVal = dataMap.get(key);
        if( curVal == null || !(curVal instanceof LockedValue ) ) {
            throw new IllegalMonitorStateException("Key " + key + " is not locked");
        }
        
        @SuppressWarnings("unchecked")
        LockedValue<V> lockedValue = ( LockedValue<V> ) curVal;
        unlock(key, lockedValue);
    }
    
    /**
     * Unlock the specified key after changing its value to be newValue. If newValue
     * is null this is equivalent to <code>map.remove(key); map.unlock(key);</code>.
     * 
     * @param key the key to unlock 
     * @param newValue the new value for the key
     */
    public void unlockWithNewValue(K key, V newValue) {
        LockedValue<V> lockedValue = getLockedValue( key );
        lockedValue.setValue(newValue);
        unlock( key, lockedValue );
    }

    private void unlock(K key, LockedValue<?> lockedValue) {
        try {
            int holdCount = lockedValue.getHoldCount();
            if( holdCount == 0 ) {
                throw new IllegalMonitorStateException("Key " + key + " is not locked");
            }
            
            // This could miss newly arriving threads but that's okay as they will check
            // the map again after getting the lock. We could do this after every call to unlock
            // where holdCount == 1 without breaking correctness; it would just make the locking
            // less fair. Its also possible that this could leave a lock around with nothing waiting
            // on it if a thread stops waiting for the lock without acquiring it but that shouldn't 
            // happen as this class doesn't use lockInterruptably or tryLock and even if it does
            // the lock will be cleaned up the next time the value is checked
            if( holdCount == 1 && !lockedValue.hasQueuedThreads() ) {
                boolean result = doReplace(key, lockedValue, lockedValue.getValue());
                assert result : "Map changed unexpectedly";
            }
        } finally {
            lockedValue.unlock();
        }
    }
    
    private boolean doReplace(K key, Object current, Object newVal) {
        if( current == null ) {
            if( newVal == null ) {
                return !dataMap.containsKey(key);
            } else {
                return dataMap.putIfAbsent(key, newVal) == null;
            }
        } else if( newVal == null ) {
            return dataMap.remove(key, current);
        } else {
            return dataMap.replace(key, current, newVal);
        }
    }
    
    /**
     * Get the real value for the given key provided the 
     * current map value. This will wait for the lock to be
     * released if the key is currently locked
     * 
     * @param key the key
     * @param currentLock the expected current value
     * @return
     */
    @SuppressWarnings("unchecked")
    @CheckForNull
    private V getRealValue(Object key, Object currentMapValue) {
        Object curVal = currentMapValue;
        while( true ) {
            if( !( curVal instanceof LockedValue ) ) {
                return (V) curVal;
            }
            
            LockedValue<V> lockedValue = ( LockedValue<V> ) curVal;
            
            lockedValue.lock();
            if( dataMap.get(key) == lockedValue ) {
                V value = lockedValue.getValue();
                unlock( (K) key, lockedValue);
                return value;
            } else {
                lockedValue.unlock();
            }
            
            curVal = dataMap.get(key);
        }
    }
    
    /**
     * Clear the map. This will automatically acquire all the locks needed to clear the map. Also 
     * note that this function does not block parallel writes adding to the map while it is in
     * progress.
     */
    @Override
    public void clear() {
        Iterator<Map.Entry<K,Object>> itr = dataMap.entrySet().iterator();
        while( itr.hasNext() ) {
            Map.Entry<K, Object> entry = itr.next();
            
            // Only lock if someone else is already locking or the value changed while
            //  it was being removed
            if( entry.getValue() instanceof LockedValue ||
                !dataMap.remove( entry.getKey(), entry.getValue() )) 
            {
                lock( entry.getKey() );
                remove( entry.getKey() );
                unlock( entry.getKey() );
            }
        }
    }
    
    /**
     * Return if the map contains the specified key. This does not wait for locks
     * and returns true if the key is locked even if it has no value 
     */
    @Override
    public boolean containsKey(Object arg0) {
        return dataMap.containsKey(arg0);
    }
    
    /**
     * Return if the map contains the specified values. To ensure partially created
     * objects are not compared this function will wait for the lock for the matching
     * key to be released before doing the comparison
     */
    @Override
    public boolean containsValue(Object value) {
        if( value == null ) return false;
        
        for( Map.Entry<K, Object> entry : dataMap.entrySet() ) {
            V entryValue = getRealValue( entry.getKey(), entry.getValue() );
            if( entryValue == null ) continue;
            if( value.equals( entryValue ) ) return true;
        }
        
        return false;
    }
    
    @Override
    public V get(Object key) {
        return getRealValue( key, dataMap.get(key) );
    }
    
    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        if( entrySet == null ) {
            entrySet = new EntrySet();
        }
        return entrySet;
    }
    
    @Override
    public boolean isEmpty() {
        return dataMap.isEmpty();
    }
    
    @Override
    public Set<K> keySet() {
        return dataMap.keySet();
    }
    
    /**
     * Get a value the locked value for the specified key. Throws {@link IllegalMonitorStateException}
     * if the key is not locked or is not locked by the current thread.
     */
    private LockedValue<V> getLockedValue(Object key)
        throws IllegalMonitorStateException
    {
        Object curValOrLock = dataMap.get(key);
        
        if( !(curValOrLock instanceof LockedValue ) ) {
            throw new IllegalMonitorStateException("Key " + key + " is not locked.");
        }
        
        @SuppressWarnings("unchecked")
        LockedValue<V> lockedValue = (LockedValue<V>) curValOrLock;
        if( !lockedValue.isHeldByCurrentThread() ) {
            throw new IllegalMonitorStateException("Key " + key + " is not locked."); 
        }
        
        return lockedValue;
    }
    
    @Override
    public V put(K key, V value) {
        LockedValue<V> lockedValue = getLockedValue(key);
        V oldValue = lockedValue.getValue();
        lockedValue.setValue(value);
        return oldValue; 
    }
    
    @Override
    public V remove(Object key) {
        LockedValue<V> lockedValue = getLockedValue(key);
        V oldValue = lockedValue.getValue();
        lockedValue.setValue(null);
        return oldValue; 
    }
    
    /**
     * Return the size of the map. Note that locked entries are counted towards the size even if
     * they have no value.
     */
    @Override
    public int size() {
        return dataMap.size();
    }
}
