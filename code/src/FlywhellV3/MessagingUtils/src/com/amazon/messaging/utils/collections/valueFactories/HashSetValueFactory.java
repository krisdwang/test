package com.amazon.messaging.utils.collections.valueFactories;

import java.util.HashSet;

import com.amazon.messaging.utils.collections.ValueFactory;

public class HashSetValueFactory<V> implements ValueFactory<HashSet<V> > {
    @Override
    public HashSet<V> createValue() {
        return new HashSet<V>();
    }
}
