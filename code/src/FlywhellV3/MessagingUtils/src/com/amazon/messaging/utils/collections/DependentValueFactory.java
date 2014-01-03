package com.amazon.messaging.utils.collections;

/**
 * A value factory where the value depends on the key
 * @author stevenso
 *
 * @param <K>
 * @param <V>
 */
public interface DependentValueFactory<K,V> {
    public V createValue( K key );
}