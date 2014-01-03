package com.amazon.messaging.utils.collections;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A concurrent version of {@link FactoryMap}. This class is thread safe as long as 
 * the factory is thread safe.
 * <p>
 * If the concurrent factory map is created with ensureUniqueCreation is set to true
 * then it is guaranteed that the factory will not be called twice for the same key. You
 * should only use this map with ensureUniqueCreation if the values created by the factory
 * are cheap to create and it doesn't matter if there are a few duplicates created for
 * a key that aren't ever used.
 *  
 * @author stevenso
 * 
 * @param <K>
 * @param <V>
 */
public class ConcurrentFactoryMap<K,V> implements ConcurrentMap< K, V > {
    private final ConcurrentMap< K, V > wrappedMap;
    private final DependentValueFactory<K, V> factory;
    private final Object creationLock;

    /**
     * Create a ConcurrentFactoryMap.
     * 
     * @param wrappedMap the concurrent map to wrap
     * @param factory the factory to use to create values
     * @param ensureUniqueCreation if true it is guaranteed that factory will not
     *   be called twice for the same key for this map.
     */
    public ConcurrentFactoryMap(ConcurrentMap<K, V> wrappedMap, DependentValueFactory<K, V> factory,
                                boolean ensureUniqueCreation)
    {
        this.wrappedMap = wrappedMap;
        this.factory = factory;
        if( ensureUniqueCreation ) creationLock = new Object();
        else creationLock = null;
    }
    
    /**
     * Create a ConcurrentFactoryMap.
     * 
     * @param wrappedMap the concurrent map to wrap
     * @param factory the factory to use to create values
     * @param ensureUniqueCreation if true it is guaranteed that factory will not
     *   be called twice for the same key for this map.
     */
    public ConcurrentFactoryMap(ConcurrentMap<K, V> wrappedMap, ValueFactory<V> factory,
                                boolean ensureUniqueCreation)
    {
        this.wrappedMap = wrappedMap;
        this.factory = new ValueFactoryToDepedentValueFactoryAdaptor<K,V>( factory );
        if( ensureUniqueCreation ) creationLock = new Object();
        else creationLock = null;
    }
    
    /**
     * Create a ConcurrentFactoryMap. Using a concurrentHashMap
     * 
     * @param factory the factory to use to create values
     * @param ensureUniqueCreation if true it is guaranteed that factory will not
     *   be called twice for the same key for this map.
     */
    public ConcurrentFactoryMap(ValueFactory<V> factory, boolean ensureUniqueCreation)
    {
        this.wrappedMap = new ConcurrentHashMap<K, V>();
        this.factory = new ValueFactoryToDepedentValueFactoryAdaptor<K,V>( factory );
        if( ensureUniqueCreation ) creationLock = new Object();
        else creationLock = null;
    }
    
    /**
     * If true then this map is guaranteed not to call the factory twice for the same
     *  key.
     */
    public boolean isEnsureUniqueCreation() {
        return creationLock != null;
    }

    public void clear() {
        wrappedMap.clear();
    }
    
    /**
     * Returns true if the wrapped map currently contains the specified key. 
     */
    public boolean containsKey(Object key) {
        return wrappedMap.containsKey(key);
    }
    
    /**
     * Returns true if the wrapped map currently contains the specified value. If the
     * map does not contain value this function will return false even if that value
     * would be returned by the factory.
     */
    public boolean containsValue(Object value) {
        return wrappedMap.containsValue(value);
    }
    
    public Set<java.util.Map.Entry<K, V>> entrySet() {
        return wrappedMap.entrySet();
    }
    
    @Override
    public int hashCode() {
        return wrappedMap.hashCode();
    }
    
    /**
     * Returns true if o is Map that contains the same entries this map contains. 
     * The factory, if any, of o is ignored.
     */
    @Override
    public boolean equals(Object o) {
        if( o == this ) return true;
        
        return wrappedMap.equals(o);
    }
    
    public void createValueIfRequired(K key) {
        if( !wrappedMap.containsKey( key ) ) {
            V value = factory.createValue( key );
            wrappedMap.putIfAbsent( key, value );
        }
    }
    
    /**
     * Get the associated value from the map. If the map does not already contain key
     * then a value for the key is created using the factory and is inserted into the
     * map and returned.
     * <p>
     * Note that values created by the factory in response to this function being called
     * may not end up being used if another thread adds the key to the map before
     * its inserted by this call.
     */
    @SuppressWarnings("unchecked")
    public V get(Object key) {
        if( creationLock != null ) {
            if( !wrappedMap.containsKey( key ) ) {
                synchronized (creationLock) {
                    createValueIfRequired((K) key);
                }
            }
        } else {
            createValueIfRequired((K) key);
        }
        
        return wrappedMap.get(key);
    }
    
    /**
     * Get the specified value from the map without creating it.
     */
    public V getNoCreate(K key) {
        return wrappedMap.get(key);
    }
    
    public boolean isEmpty() {
        return wrappedMap.isEmpty();
    }
    
    public Set<K> keySet() {
        return wrappedMap.keySet();
    }
    
    public V put(K key, V value) {
        return wrappedMap.put(key, value);
    }
    
    public void putAll(Map<? extends K, ? extends V> m) {
        wrappedMap.putAll(m);
    }
    
    public V putIfAbsent(K key, V value) {
        return wrappedMap.putIfAbsent(key, value);
    }
    
    public boolean remove(Object key, Object value) {
        return wrappedMap.remove(key, value);
    }
    
    public V remove(Object key) {
        return wrappedMap.remove(key);
    }
    
    /**
     * Replace the entry for key only if it is currently mapped to oldVlaue. If the
     * key is not currently mapped to a value the factory is not checked to see 
     * if its return value would match oldValue.
     **/
    public boolean replace(K key, V oldValue, V newValue) {
        return wrappedMap.replace(key, oldValue, newValue);
    }
    
    public V replace(K key, V value) {
        return wrappedMap.replace(key, value);
    }
    
    public int size() {
        return wrappedMap.size();
    }
    
    public Collection<V> values() {
        return wrappedMap.values();
    }
    
    @Override
    public String toString() {
        return wrappedMap.toString();
    }
}
