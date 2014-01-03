package com.amazon.messaging.seqstore.v3.store;

import com.amazon.messaging.impls.SettableClock;

public class TestingLastAvailableClock extends StoreSpecificClock {
    public TestingLastAvailableClock() {
        super( new SettableClock() );
    }
    
    public void setCurrentTime(long time) {
        (( SettableClock ) getLocalClock() ).setCurrentTime( time );
    }
    
    public void increaseTime(long interval) {
        (( SettableClock ) getLocalClock() ).increaseTime( interval );
    }
}
