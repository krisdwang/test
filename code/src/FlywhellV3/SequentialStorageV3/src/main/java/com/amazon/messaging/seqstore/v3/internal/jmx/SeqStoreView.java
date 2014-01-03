package com.amazon.messaging.seqstore.v3.internal.jmx;


import java.io.IOException;

import net.jcip.annotations.Immutable;

import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreInternalInterface;

@Immutable
public class SeqStoreView extends SeqStoreViewBase implements SeqStoreViewMBean {
    public SeqStoreView(SeqStoreInternalInterface<?> store) {
        super(store);
    }
    
    @Override
    public long getNumberOfBuckets() throws IOException {
        try {
            return ( ( SeqStoreInternalInterface<?> ) getStore() ).getNumBuckets();
        } catch (SeqStoreClosedException e) {
            throw new IOException( "Store is already closed", e );
        }
    }
}
