/**
 * 
 */
package com.amazon.messaging.utils.collections;

/**
 * A factory used to create values for FactoryMap.
 * 
 * @author stevenso
 *
 * @param <V>
 */
public interface ValueFactory<V> {
    public V createValue();
}