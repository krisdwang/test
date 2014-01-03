package com.amazon.messaging.utils.collections;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.RandomAccess;
import java.util.SortedSet;

/**
 * A read only set based on a binary search of a fixed list 
 * 
 * @author stevenso
 *
 */
public class BinarySearchSet<T> extends AbstractSet<T> implements NavigableSet<T>, Serializable {
    private static final long serialVersionUID = 1L;

    private final class DescendingSet extends AbstractSet<T> implements NavigableSet<T>, Serializable {
        private static final long serialVersionUID = 1L;
        
        private final Comparator<? super T> comparator;
        
        public DescendingSet() {
            comparator = Collections.reverseOrder( BinarySearchSet.this.comparator );
        }

        @Override
        public Comparator<? super T> comparator() {
            return comparator;
        }

        @Override
        public T first() {
            return BinarySearchSet.this.last();
        }

        @Override
        public T last() {
            return BinarySearchSet.this.first();
        }

        @Override
        public int size() {
            return BinarySearchSet.this.size();
        }

        @Override
        public boolean isEmpty() {
            return BinarySearchSet.this.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return BinarySearchSet.this.contains(o);
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return BinarySearchSet.this.containsAll( c );
        }
        
        @Override
        public T lower(T e) {
            return BinarySearchSet.this.higher(e);
        }

        @Override
        public T floor(T e) {
            return BinarySearchSet.this.ceiling(e);
        }

        @Override
        public T ceiling(T e) {
            return BinarySearchSet.this.floor(e);
        }

        @Override
        public T higher(T e) {
            return BinarySearchSet.this.lower(e);
        }

        @Override
        public NavigableSet<T> descendingSet() {
            return BinarySearchSet.this;
        }

        @Override
        public Iterator<T> descendingIterator() {
            return BinarySearchSet.this.iterator();
        }

        @Override
        public NavigableSet<T> subSet(T fromElement, boolean fromInclusive, T toElement, boolean toInclusive) {
            return BinarySearchSet.this.subSet(toElement, toInclusive, fromElement, fromInclusive).descendingSet();
        }

        @Override
        public NavigableSet<T> headSet(T toElement, boolean inclusive) {
            return BinarySearchSet.this.tailSet(toElement, inclusive).descendingSet();
        }

        @Override
        public NavigableSet<T> tailSet(T fromElement, boolean inclusive) {
            return BinarySearchSet.this.headSet(fromElement, inclusive).descendingSet();
        }

        @Override
        public SortedSet<T> subSet(T fromElement, T toElement) {
            return subSet(fromElement, true, toElement, false);
        }

        @Override
        public SortedSet<T> headSet(T toElement) {
            return headSet(toElement, false);
        }

        @Override
        public SortedSet<T> tailSet(T fromElement) {
            return tailSet(fromElement, true);
        }

        @Override
        public Iterator<T> iterator() {
            return BinarySearchSet.this.descendingIterator();
        }
        
        @Override
        public boolean add(T e) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(Collection<? extends T> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }

        @Override
        public T pollFirst() {
            throw new UnsupportedOperationException();
        }

        @Override
        public T pollLast() {
            throw new UnsupportedOperationException();
        }
    }
    
    private static class DescendingIterator<T> implements Iterator<T> {
        private final ListIterator<T> itr;
        
        public DescendingIterator(List<T> values) {
            itr = values.listIterator( values.size() );
        }
            
        @Override
        public boolean hasNext() { return itr.hasPrevious(); }
        
        @Override
        public T next() { return itr.previous(); }

        @Override
        public void remove() { throw new UnsupportedOperationException(); }
    }
    
    private final List<T> values;
    
    private final Comparator<? super T> comparator;
    
    private transient volatile DescendingSet descendingSet;
    
    public BinarySearchSet(List<T> values) {
        this( values, null );
    }
    
    public BinarySearchSet(List<T> values, Comparator<? super T> comparator) {
        if( !(values instanceof RandomAccess) ) 
            throw new IllegalArgumentException("List must be random access list");
        
        this.comparator = comparator;
        this.values = values;
        this.descendingSet = null;
    }
    
    @Override
    public Comparator<? super T> comparator() {
        return comparator;
    }
    
    @Override
    public T first() {
        if( values.isEmpty() ) throw new NoSuchElementException();
        return values.get( 0 );
    }

    @Override
    public T last() {
        if( values.isEmpty() ) throw new NoSuchElementException();
        return values.get( values.size() - 1) ;
    }

    @Override
    public int size() {
        return values.size();
    }

    @Override
    public boolean isEmpty() {
        return values.isEmpty();
    }
    
    private int search(T val) {
        return Collections.binarySearch(values, val, comparator );
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean contains(Object o) {
        return search((T) o ) >= 0; 
    }

    @Override
    public Object[] toArray() {
        return values.toArray();
    }

    @Override
    public <V> V[] toArray(V[] a) {
        return values.toArray(a);
    }

    @Override
    public T lower(T e) {
        int index = search(e);
        
        if( index == 0 ) return null;
        if( index > 0 ) return values.get( index - 1);
        
        int prev = -index - 2;
        if( prev < 0 ) return null;
        return values.get( prev );
    }

    @Override
    public T floor(T e) {
        int index = search(e);
        
        if( index >= 0 ) return values.get( index );
        
        int prev = -index - 2;
        if( prev < 0 ) return null;
        return values.get( prev );
    }

    @Override
    public T ceiling(T e) {
        int index = search(e);
        
        if( index >= 0 ) return values.get( index );
        
        int next = -index - 1;
        if( next >= values.size() ) return null;
        return values.get( next );
    }

    @Override
    public T higher(T e) {
        int index = search(e);
        
        int next;
        if( index >= 0 ) next = index + 1;
        else next = -index - 1;
        
        if( next >= values.size() ) return null;
        return values.get( next );
    }

    @Override
    public Iterator<T> iterator() {
        return values.iterator();
    }

    @Override
    public NavigableSet<T> descendingSet() {
        if( descendingSet == null ) descendingSet = new DescendingSet();
        return descendingSet;
    }

    @Override
    public Iterator<T> descendingIterator() {
        return new DescendingIterator<T>(values);
    }

    @Override
    public NavigableSet<T> subSet(T fromElement, boolean fromInclusive, T toElement, boolean toInclusive) {
        int fromIndex = ceilIndex(fromElement, fromInclusive);
        int toIndex = floorIndex(toElement, toInclusive);
        if( toIndex < values.size() ) toIndex++;
        
        if( fromIndex > toIndex ) toIndex = fromIndex; // Empty list
        return new BinarySearchSet<T>( values.subList(fromIndex, toIndex), comparator );
    }

    /**
     * Return the index of greatest element in the set less than, or equal to if inclusive is true, 
     * element. If there is no such element this function returns -1
     */
    private int floorIndex(T element, boolean inclusive) {
        int index = search( element );
        if( index < 0 ) {
            index = -index - 2;
            assert( index >= -1 );
        } else if( !inclusive ) {
            index--;
        }
        return index;
    }

    /**
     *  Return the least element in the set greater than, or equal to if inclusive is true, 
     *  element.
     */
    private int ceilIndex(T element, boolean inclusive) {
        int index = search(element);
        if( index < 0 ) {
            index = -index - 1;
        } else if( !inclusive ) {
            index++;
        }
        assert(index <= values.size() );
        
        return index;
    }

    @Override
    public NavigableSet<T> headSet(T toElement, boolean inclusive) {
        int toIndex = floorIndex(toElement, inclusive);
        if( toIndex < values.size() ) toIndex++;
        
        return new BinarySearchSet<T>(values.subList(0, toIndex), comparator );
    }

    @Override
    public NavigableSet<T> tailSet(T fromElement, boolean inclusive) {
        int fromIndex = ceilIndex(fromElement, inclusive);
        return new BinarySearchSet<T>(values.subList(fromIndex, values.size()), comparator );
    }

    @Override
    public SortedSet<T> subSet(T fromElement, T toElement) {
        return subSet(fromElement, true, toElement, false);
    }

    @Override
    public SortedSet<T> headSet(T toElement) {
        return headSet(toElement, false);
    }

    @Override
    public SortedSet<T> tailSet(T fromElement) {
        return tailSet(fromElement, true);
    }

    @Override
    public T pollFirst() {
        throw new UnsupportedOperationException();
    }

    @Override
    public T pollLast() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean addAll(Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean add(T e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public String toString() {
        return values.toString();
    }
}
