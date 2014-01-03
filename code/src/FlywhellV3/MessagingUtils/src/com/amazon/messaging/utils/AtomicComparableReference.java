package com.amazon.messaging.utils;

import java.util.Comparator;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An {@link AtomicReference} with the addition feature that the value can be atomicly 
 * increased or decreased. 
 * 
 * @author stevenso
 *
 * @param <T>
 */
public class AtomicComparableReference<T> extends AtomicReference<T> {
    private static final long serialVersionUID = 1L;
    
    private final Comparator< T > comparator;
    
    public AtomicComparableReference() {
        this.comparator = null;
    }
    
    public AtomicComparableReference(T initialValue) {
        super(initialValue);
        this.comparator = null;
    }
    
    public AtomicComparableReference(Comparator<T> comparator) {
        this.comparator = comparator;
    }
    
    public AtomicComparableReference(T initialValue, Comparator<T> comparator) {
        super(initialValue);
        this.comparator = comparator;
    }
    
    @SuppressWarnings("unchecked")
    private int doCompare( T oldVal, T newVal ) {
        if( comparator == null ) return ( (Comparable<T>) oldVal).compareTo( newVal );
        else return comparator.compare( oldVal, newVal );
    }

    /**
      * Atomically increase the reference's value to newValue. If the current value
      * is greater than or equal to newValue then value is not updated. A value of null
      * is always replaced.
      * 
      * @param value the value to increase the reference to
      * @return true if the value was updated
      */
     public boolean increaseTo( T newValue ) {
         for(;;) {
             T current = get();
             if( current == null || doCompare( current, newValue ) < 0 ) {
                 if( compareAndSet( current, newValue ) ) {
                     return true;
                 }
             } else {
                 return false;
             }
         }
     }
     
     /**
      * Atomically decrease the reference's value to newValue. If the current value
      * is less than or equal to newValue then value is not updated. A value of
      * null is always replaced.
      * 
      * @param value the value to increase the reference to
      * @return true if the value was updated
      */
     public boolean decreaseTo( T newValue ) {
         for(;;) {
             T current = get();
             if( current == null || doCompare( current, newValue ) > 0 ) {
                 if( compareAndSet( current, newValue ) ) {
                     return true;
                 }
             } else {
                 return false;
             }
         }
     }
}
