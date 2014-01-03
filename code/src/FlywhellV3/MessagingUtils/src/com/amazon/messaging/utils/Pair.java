package com.amazon.messaging.utils;

import java.util.Map;

import net.jcip.annotations.Immutable;

import lombok.Data;
import lombok.ToString;

/**
 * An immutable Pair class. Implements Map.Entry<K,V> so it can be used wherever
 * a Map.Entry is expected. 
 * <p>
 * This class matches the required equals and hashCode definitions for Map.Entry.
 * 
 * @author stevenso
 *
 * @param <K>
 * @param <V>
 */
@Immutable
@Data
@ToString(includeFieldNames=false)
public class Pair<K,V> implements Map.Entry<K,V> {
    private final K key;
    private final V value;
    
    @Override
    public V setValue(V arg0) {
        throw new UnsupportedOperationException( "Pair is immutable.");
    }
    
    @Override
    public boolean equals( Object o ) {
        if( this == o ) return true;
        if( o == null ) return false;
        
        if( !( o instanceof Map.Entry<?, ?> ) ) {
            return false;
        }
        
        Map.Entry<?,?> e2 = (Map.Entry<?,?>) o;
        
        // Match the required definition of Map.Entry.equals()
        return (key == null ? e2.getKey() == null : key.equals(e2.getKey())) &&
               (value == null ? e2.getValue() == null : value.equals(e2.getValue()));
    }
    
    @Override
    public int hashCode() {
        // Required definition of hashCode
        return (key == null ? 0 : key.hashCode()) ^ (value == null ? 0 : value.hashCode());
    }
    
    /**
     * Cast the pair to a new type.
     * @param <NK> the new key type
     * @param <NV> the new value type
     * @param newKeyClass the new key class 
     * @param newValueClass the new value class
     * @return this
     * @throws ClassCastException if the key and the value cannot be safely cast to the new type
     */
    @SuppressWarnings("unchecked")
    public <NK, NV> Pair<NK,NV> cast(Class<? extends NK> newKeyClass, Class<? extends NV> newValueClass) throws ClassCastException {
        newKeyClass.cast( key );
        newValueClass.cast( value );
        return ( Pair ) this;
    }
}
