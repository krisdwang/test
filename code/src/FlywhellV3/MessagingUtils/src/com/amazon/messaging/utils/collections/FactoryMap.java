package com.amazon.messaging.utils.collections;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import net.jcip.annotations.NotThreadSafe;

/**
 * A wrapper for a map that automatically creates values if they don't already exist.
 * 
 * This class is not thread safe even if the wrapped map is thread safe. 
 *  
 * @author stevenso
 *
 */
@NotThreadSafe
public class FactoryMap<K,V> implements Map<K,V>  {
    private final Map< K, V > wrappedMap;
    private final DependentValueFactory<K, V> factory;
    
    public FactoryMap(Map<K, V> wrappedMap, ValueFactory<V> factory) {
        this.wrappedMap = wrappedMap;
        this.factory = new ValueFactoryToDepedentValueFactoryAdaptor<K,V>( factory );
    }
    
    public FactoryMap(Map<K, V> wrappedMap, DependentValueFactory<K, V> factory) {
        this.wrappedMap = wrappedMap;
        this.factory = factory;
    }
    
    /**
     * Create a factory map backed by a {@link HashMap}
     * @param factory
     */
    public FactoryMap(ValueFactory<V> factory) {
        this.wrappedMap = new HashMap<K, V>();
        this.factory = new ValueFactoryToDepedentValueFactoryAdaptor<K,V>( factory );
    }
    
    /**
     * Create a factory map backed by a {@link HashMap}
     * @param factory
     */
    public FactoryMap(DependentValueFactory<K, V> factory) {
        this.wrappedMap = new HashMap<K, V>();
        this.factory = factory;
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

    /**
     * Returns true if o is Map that contains the same entries this map contains. 
     * The factory, if any, of o is ignored.
     */
    @Override
    public boolean equals(Object o) {
        if( o == this ) return true;
        return wrappedMap.equals(o);
    }
    
    @Override
    public int hashCode() {
        return wrappedMap.hashCode();
    }

    /**
     * Get the associated value from the map. If the map does not already contain key
     * then a value for the key is created using the factory and is inserted into the
     * map and returned.
     */
    @SuppressWarnings("unchecked")
    public V get(Object key) {
        if( !wrappedMap.containsKey( key ) ) {
            V value = factory.createValue( ( K ) key );
            wrappedMap.put( ( K ) key, value );
            return value;
        }
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

    public V remove(Object key) {
        return wrappedMap.remove(key);
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
