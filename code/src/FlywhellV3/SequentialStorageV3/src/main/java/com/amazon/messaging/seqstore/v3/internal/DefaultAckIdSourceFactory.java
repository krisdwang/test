package com.amazon.messaging.seqstore.v3.internal;

import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;


public class DefaultAckIdSourceFactory implements AckIdSourceFactory {
    private final AckIdGenerator ackIdGenerator;
    
    private final Clock clock;
    
    public DefaultAckIdSourceFactory(AckIdGenerator ackIdGenerator, Clock clock) {
        super();
        this.ackIdGenerator = ackIdGenerator;
        this.clock = clock;
    }
    
    @Override
    public AckIdSource createAckIdSource(StoreId storeId) {
        return new DefaultAckIdSource(ackIdGenerator, clock);
    }
}
