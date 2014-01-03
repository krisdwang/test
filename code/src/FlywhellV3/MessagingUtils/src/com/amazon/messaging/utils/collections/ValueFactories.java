package com.amazon.messaging.utils.collections;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentMap;

import com.amazon.messaging.utils.collections.valueFactories.ArrayListValueFactory;
import com.amazon.messaging.utils.collections.valueFactories.ConcurrentHashMapValueFactory;
import com.amazon.messaging.utils.collections.valueFactories.HashMapValueFactory;
import com.amazon.messaging.utils.collections.valueFactories.HashSetValueFactory;
import com.amazon.messaging.utils.collections.valueFactories.TreeSetValueFactory;

/**
 * Utility class for getting instances of common value factories.
 * 
 * @author stevenso
 *
 */
public class ValueFactories {
    @SuppressWarnings("unchecked")
    private static final ValueFactory<? extends List> listFactory = new ArrayListValueFactory();
    
    /**
     * Returns a ValueFactory that creates ArrayLists.
     */
    @SuppressWarnings("unchecked")
    public static <V> ValueFactory< List< V > > getListFactory() {
        return (ValueFactory<List<V>>) listFactory;
    }
    
    @SuppressWarnings("unchecked")
    private static final ValueFactory<? extends ConcurrentMap> concurrentMapFactory = new ConcurrentHashMapValueFactory();
    
    /**
     * Returns a ValueFactory that creates ConcurrentHashMaps.
     */
    @SuppressWarnings("unchecked")
    public static <K, V> ValueFactory< ConcurrentMap<K, V> > getConcurrentMapFactory() {
        return (ValueFactory<ConcurrentMap<K, V>>) concurrentMapFactory;
    }
    
    @SuppressWarnings("unchecked")
    private static final ValueFactory<? extends Map> mapFactory = new HashMapValueFactory();
    
    /**
     * Returns a ValueFactory that creates HashMaps.
     */
    @SuppressWarnings("unchecked")
    public static <K, V> ValueFactory< Map<K, V> > getMapFactory() {
        return (ValueFactory<Map<K, V>>) mapFactory;
    }
    
    @SuppressWarnings("unchecked")
    private static final ValueFactory<? extends Set> setFactory = new HashSetValueFactory();
    
    /**
     * Get an instance of this factory.
     */
    @SuppressWarnings("unchecked")
    public static <V> ValueFactory< Set< V > >  getSetFactory() {
        return (ValueFactory<Set<V>>) setFactory;
    }
    
    @SuppressWarnings("unchecked")
    private static final ValueFactory<? extends SortedSet> navigableSetSetFactory = new TreeSetValueFactory();
    
    /**
     * Returns a ValueFactory that creates TreeSets that use the default comparator
     */
    @SuppressWarnings("unchecked")
    public static <V> ValueFactory< SortedSet< V > > getSortedSetFactory() {
        return (ValueFactory<SortedSet<V>>) navigableSetSetFactory;
    }

    /**
     * Returns a ValueFactory that creates TreeSets that use the provided comparator
     */
    @SuppressWarnings("unchecked")
    public static <V> ValueFactory< SortedSet< V > >  getSortedSetFactory(Comparator<V> comparator) {
        return (ValueFactory) new TreeSetValueFactory<V>(comparator);
    }
    
    /**
     * Returns a ValueFactory that creates TreeSets that use the default comparator
     */
    @SuppressWarnings("unchecked")
    public static <V> ValueFactory< NavigableSet< V > > getNavigableSetFactory() {
        return (ValueFactory<NavigableSet<V>>) navigableSetSetFactory;
    }

    /**
     * Returns a ValueFactory that creates TreeSets that use the provided comparator
     */
    @SuppressWarnings("unchecked")
    public static <V> ValueFactory< NavigableSet< V > >  getNavigableSetFactory(Comparator<V> comparator) {
        return (ValueFactory) new TreeSetValueFactory<V>(comparator);
    }
}
