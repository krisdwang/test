package com.amazon.messaging.utils.collections.valueFactories;

import java.util.ArrayList;

import com.amazon.messaging.utils.collections.ValueFactory;

public class ArrayListValueFactory<V> implements ValueFactory<ArrayList<V>> {
    @Override
    public ArrayList<V> createValue() {
        return new ArrayList<V>();
    }
}
