package com.amazon.messaging.seqstore.v3.util;

import java.util.Iterator;

import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;

public class StringToStoreIdIterator implements Iterator<StoreId> {
    private final Iterator<String> stringItr;

    public StringToStoreIdIterator(Iterator<String> stringItr) {
        this.stringItr = stringItr;
    }

    @Override
    public boolean hasNext() {
        return stringItr.hasNext();
    }

    @Override
    public StoreId next() {
        return new StoreIdImpl( stringItr.next() );
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}