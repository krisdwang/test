package com.amazon.messaging.seqstore.v3.internal;

import java.util.concurrent.atomic.AtomicLong;

import com.amazon.messaging.annotations.TestOnly;

public class AckIdGenerator {

    private final AtomicLong count;

    private final int restartTime;
    
    public AckIdGenerator(long time) {
        this.restartTime = (int) (time / 1000);
        this.count = new AtomicLong(0);
    }

    @TestOnly
    public AckIdGenerator() {
        this(System.currentTimeMillis() / 1000);
    }
    
    @TestOnly
    public AckIdGenerator(AckIdGenerator copy) {
        this.restartTime = copy.restartTime;
        count = new AtomicLong( copy.count.get() );
    }
    
    public AckIdV3 getAckId(long time) {
        long seq = count.getAndIncrement();
        return new AckIdV3(time, seq, restartTime);
    }

    public AckIdV3 getAckId(long time, long seq) {
        return new AckIdV3(time, seq, restartTime);
    }

    public AckIdV3 next(long time) {
        long seq = count.getAndIncrement();
        return new AckIdV3(time, seq, restartTime);
    }
}
