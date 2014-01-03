package com.amazon.messaging.utils.collections.valueFactories;

import java.util.concurrent.ConcurrentHashMap;

import com.amazon.messaging.utils.collections.ValueFactory;

public class ConcurrentHashMapValueFactory<K,V> implements ValueFactory<ConcurrentHashMap<K, V>> {
    @Override
    public ConcurrentHashMap<K,V> createValue() {
        return new ConcurrentHashMap<K, V>();
    }
}
