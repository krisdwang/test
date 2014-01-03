package com.amazon.messaging.utils.collections;

import java.util.HashMap;
import java.util.Map;

public class FactoryMapTest extends FactoryMapTestBase {

    @Override
    protected <K, V> Map<K, V> getFactoryMap(ValueFactory<V> factory) {
        return new FactoryMap<K, V>( new HashMap<K, V>(), factory );
    }

    @Override
    protected <K, V> Map<K, V> getFactoryMap(DependentValueFactory<K, V> factory) {
        return new FactoryMap<K, V>( new HashMap<K, V>(), factory );
    }
    
}
