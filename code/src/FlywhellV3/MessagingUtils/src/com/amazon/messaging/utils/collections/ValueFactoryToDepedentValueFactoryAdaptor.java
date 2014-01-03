/**
 * 
 */
package com.amazon.messaging.utils.collections;

class ValueFactoryToDepedentValueFactoryAdaptor<K, V> implements DependentValueFactory<K,V> {
    private final ValueFactory<V> factory;
    
    public ValueFactoryToDepedentValueFactoryAdaptor(ValueFactory<V> factory) {
        this.factory = factory;
    }

    @Override
    public V createValue(K key) {
        return factory.createValue();
    }
}