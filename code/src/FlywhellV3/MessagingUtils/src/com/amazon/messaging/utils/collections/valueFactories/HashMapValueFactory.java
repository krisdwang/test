/**
 * 
 */
package com.amazon.messaging.utils.collections.valueFactories;

import java.util.HashMap;

import com.amazon.messaging.utils.collections.ValueFactory;

public class HashMapValueFactory<K,V> implements ValueFactory< HashMap<K, V> > {
    @Override
    public HashMap<K,V> createValue() {
        return new HashMap<K, V>();
    }
}