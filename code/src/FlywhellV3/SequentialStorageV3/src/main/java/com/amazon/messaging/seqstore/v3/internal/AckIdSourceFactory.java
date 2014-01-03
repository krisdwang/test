package com.amazon.messaging.seqstore.v3.internal;

import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;


public interface AckIdSourceFactory {
    /**
     * Create an {@link AckIdSource} for the given store.
     * 
     * @param storeId 
     * @return a new {@link AckIdSource}
     */
    public AckIdSource createAckIdSource(StoreId storeId);
}
