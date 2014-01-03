package com.amazon.messaging.utils;

import java.util.Comparator;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;


public class AtomicComparableReferenceUtils {
    /**
     * Atomically increase the reference's value to newValue. If the current value
     * is greater than or equal to newValue then value is not updated. A value of null
     * is always replaced.
     * 
     * @param fieldUpdater the {@link AtomicReferenceFieldUpdater} to use to 
     *      update the value
     * @param object the object containing the field to update
     * @param value the value to increase the reference to
     * @return true if the value was updated
     */
    public static <T, V extends Comparable<V>> boolean increaseTo( 
        AtomicReferenceFieldUpdater<T, V> fieldUpdater,
        T object, V value ) 
    {
        for(;;) {
            V current = fieldUpdater.get(object);
            if( current == null || current.compareTo( value ) < 0 ) {
                if( fieldUpdater.compareAndSet(object, current, value) ) {
                    return true;
                }
            } else {
                return false;
            }
        }
    }
    
    /**
     * Atomically increase the reference's value to newValue. If the current value
     * is greater than or equal to newValue then value is not updated. A value of null
     * is always replaced.
     * 
     * @param fieldUpdater the {@link AtomicReferenceFieldUpdater} to use to 
     *      update the value
     * @param object the object containing the field to update
     * @param comparator the comparator to use to compare values
     * @param value the value to increase the reference to
     * @return true if the value was updated
     */
    public static <T, V> boolean increaseTo( 
        AtomicReferenceFieldUpdater<T, V> fieldUpdater, 
        Comparator<V> comparator,
        T object, V value ) 
    {
        for(;;) {
            V current = fieldUpdater.get(object);
            if( current == null || comparator.compare( current, value ) < 0 ) {
                if( fieldUpdater.compareAndSet(object, current, value) ) {
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
     * @param fieldUpdater the {@link AtomicReferenceFieldUpdater} to use to 
     *      update the value
     * @param object the object containing the field to update
     * @param value the value to decrease the reference to
     * @return true if the value was updated
     */
    public  static <T, V extends Comparable<V>> boolean decreaseTo(  
        AtomicReferenceFieldUpdater<T, V> fieldUpdater,
        T object, V value ) 
    {
        for(;;) {
            V current = fieldUpdater.get(object);
            if( current == null || current.compareTo( value ) > 0 ) {
                if( fieldUpdater.compareAndSet(object, current, value) ) {
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
     * @param fieldUpdater the {@link AtomicReferenceFieldUpdater} to use to 
     *      update the value
     * @param object the object containing the field to update
     * @param comparator the comparator to use to compare values
     * @param value the value to decrease the reference to
     * @return true if the value was updated
     */
    public static <T,V> boolean decreaseTo(  
        AtomicReferenceFieldUpdater<T, V> fieldUpdater,
        Comparator<V> comparator,
        T object, V value ) 
    {
        for(;;) {
            V current = fieldUpdater.get(object);
            if( current == null || comparator.compare( current, value ) > 0 ) {
                if( fieldUpdater.compareAndSet(object, current, value) ) {
                    return true;
                }
            } else {
                return false;
            }
        }
    }
}
