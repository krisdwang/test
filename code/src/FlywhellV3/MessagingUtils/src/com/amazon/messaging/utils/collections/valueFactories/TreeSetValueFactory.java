package com.amazon.messaging.utils.collections.valueFactories;

import java.util.Comparator;
import java.util.TreeSet;

import com.amazon.messaging.utils.collections.ValueFactory;

/**
 * Utility factory that creates TreeSets for use with FactoryMap.
 * @author stevenso
 *
 * @param <V>
 */
public class TreeSetValueFactory<V> implements ValueFactory< TreeSet< V > > {
    private final Comparator<V> comparator;
    
    public TreeSetValueFactory() {
        this(null);
    }
    
    public TreeSetValueFactory( Comparator<V> comparator) {
        this.comparator = comparator;
    }
    
    @Override
    public TreeSet<V> createValue() {
        return new TreeSet<V>(comparator);
    }
}