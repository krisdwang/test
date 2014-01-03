package com.amazon.messaging.utils.collections.valueFactories;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.amazon.messaging.utils.collections.ValueFactory;

public class SynchronizedSetValueFactory<V> implements ValueFactory<Set<V> > {
    @Override
    public Set<V> createValue() {
        return Collections.synchronizedSet(new HashSet<V>());
    }
}
