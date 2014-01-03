package com.amazon.messaging.utils.collections;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import lombok.Data;

public class CollectionUtils {
    @Data
    private static class OrderedKey<T> {
        private final T key;
        
        private final int index;
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static <T> int compare(T o1, T o2, Comparator<? super T> comparator) {
        if( comparator != null ) return comparator.compare( o1, o2 );
        return ( ( Comparable ) o1 ).compareTo( o2 );
    }
    
    private static class OrderedKeyCompartor<T> implements Comparator<OrderedKey<T>> {
        private final Comparator<? super T> comparator;
        
        public OrderedKeyCompartor(Comparator<? super T> comparator) {
            super();
            this.comparator = comparator;
        }

        @Override
        public int compare(OrderedKey<T> o1, OrderedKey<T> o2) {
            if( o1 == o2 ) return 0;
            
            int keyCompare = CollectionUtils.compare( o1.key, o2.key, comparator );
            if( keyCompare != 0 ) return keyCompare;
            
            return o1.index - o2.index;
        }
    }
    
    /**
     * Get an unmodifiable NavigableSet that represents the merger of all of the given sets.
     * All the provided sets must have the equal comparators.
     * <p>
     * This function runs in O(n log m) time where n is the total number of entries in all sets 
     * and m is the number of sets.
     * <p>
     * If you want a modifiable set the {@link TreeSet#TreeSet(SortedSet)} constructor
     * will construct a TreeSet from the returned set in O(n) time where n is the number of elements
     * in the resulting set.
     * 
     * @param sets the sets to merge in the resulting set
     * @return a set that is ther merger of all of <code>sets</code>
     */
    public static <T> NavigableSet<T> mergeSets( List<? extends SortedSet<T> > sets ) { 
        if( sets.isEmpty() ) {
            return new BinarySearchSet<T>(Collections.<T>emptyList());
        }
        
        Comparator<? super T> comparator = sets.get(0).comparator();
        TreeMap<OrderedKey<T>, Iterator<T> > setItrs 
            = new TreeMap<OrderedKey<T>, Iterator<T>>( new OrderedKeyCompartor<T>( comparator ) );
        
        int totalSize = 0;
        int index = 0;
        for( SortedSet<T> set : sets ) {
            if( comparator != set.comparator() && 
                ( comparator == null || !comparator.equals( set.comparator() ) ) )
            {
                throw new IllegalArgumentException("All sets must have equal comparators");
            }
            
            Iterator<T> itr = set.iterator();
            if( itr.hasNext() ) {
                setItrs.put( new OrderedKey<T>( itr.next(), index++ ), itr );
                totalSize += set.size();
            }
        }

        List<T> values = new ArrayList<T>(totalSize);
        
        T last = null;
        boolean first = true;
        while( !setItrs.isEmpty() ) {
            Map.Entry<OrderedKey<T>, Iterator<T> > entry = setItrs.pollFirstEntry();
            T key = entry.getKey().getKey();
            
            if( first || compare(last, key, comparator) != 0 ) {
                values.add( key );
                last = key;
                first = false;
            }
            
            Iterator<T> itr = entry.getValue();
            if( itr.hasNext() ) {
                setItrs.put( new OrderedKey<T>( itr.next(), index++ ), itr );
            }
        }
        
        return new BinarySearchSet<T>(values);
    }
}
