package com.amazon.messaging.seqstore.v3.util;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Set;

import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;

/**
 * Immutable map from String to StoreId
 * @author stevenso
 *
 */
public class StringToStoreIdSet extends AbstractSet<StoreId> implements Set<StoreId> {
    private final Set<String> stringSet;

    public StringToStoreIdSet(Set<String> stringSet) {
        this.stringSet = stringSet;
    }
    
    @Override
    public boolean add(StoreId e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        return stringSet.isEmpty();
    }

    @Override
    public int size() {
        return stringSet.size();
    }

    @Override
    public boolean contains(Object o) {
        if( !( o instanceof StoreId ) ) return false;
        return stringSet.contains( ( ( StoreId ) o ).getStoreName() );
    }

    @Override
    public Iterator<StoreId> iterator() {
        return new StringToStoreIdIterator( stringSet.iterator() );
    }
}