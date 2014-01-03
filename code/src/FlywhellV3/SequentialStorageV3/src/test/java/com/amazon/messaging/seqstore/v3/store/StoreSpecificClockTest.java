package com.amazon.messaging.seqstore.v3.store;

import org.junit.Test;

import static org.junit.Assert.*;

import lombok.Setter;

import com.amazon.messaging.impls.SettableClock;
import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.testing.TestCase;


public class StoreSpecificClockTest extends TestCase {
    private final StoreId testStoreId = new StoreIdImpl("TestName");
    
    private class TestStoreSpecificClock extends StoreSpecificClock {
        @Setter
        private long storeOffset;

        public TestStoreSpecificClock(Clock localClock) {
            super(localClock);
        }
        
        @Override
        protected long getStoreTimeOffset(StoreId storeId) {
            assertEquals( testStoreId, storeId );
            return storeOffset;
        }
    }
    
    @Test
    public void testBasic() {
        SettableClock clock = new SettableClock(1);
        TestStoreSpecificClock storeSpecificClock = new TestStoreSpecificClock(clock);
        
        assertEquals( 5, storeSpecificClock.convertStoreTimeToLocalTime(testStoreId, 5) );
        assertEquals( 5, storeSpecificClock.convertLocalTimeToStoreTime(testStoreId, 5));
        assertEquals( 1, storeSpecificClock.getCurrentTime());
        assertSame( clock, storeSpecificClock.getLocalClock());
        assertEquals( 1, storeSpecificClock.getStoreClock(testStoreId).getCurrentTime() );
        
        storeSpecificClock.setStoreOffset( 1 );
        assertEquals( 4, storeSpecificClock.convertStoreTimeToLocalTime(testStoreId, 5) );
        assertEquals( 6, storeSpecificClock.convertLocalTimeToStoreTime(testStoreId, 5));
        assertEquals( 1, storeSpecificClock.getCurrentTime());
        assertSame( clock, storeSpecificClock.getLocalClock());
        assertEquals( 2, storeSpecificClock.getStoreClock(testStoreId).getCurrentTime() );
    }
    
    @Test
    public void testStoreClock() {
        SettableClock clock = new SettableClock(1);
        TestStoreSpecificClock storeSpecificClock = new TestStoreSpecificClock(clock);
        Clock storeClock = storeSpecificClock.getStoreClock(testStoreId);
        
        assertEquals( 1, storeClock.getCurrentTime() );
        storeSpecificClock.setStoreOffset( 1 );
        assertEquals( 2, storeClock.getCurrentTime() );
        storeSpecificClock.setStoreOffset( -1 );
        assertEquals( 2, storeClock.getCurrentTime() ); // Clock shouldn't go backwards
        assertEquals( 0, storeSpecificClock.getStoreClock(testStoreId).getCurrentTime() ); // but a new clock represents the new offset
        
        clock.setCurrentTime(3);
        assertEquals( 2, storeClock.getCurrentTime() ); // Clock should be paused until it doesn't have to go backwards
        
        clock.setCurrentTime(4);
        assertEquals( 3, storeClock.getCurrentTime() ); 
    }
}
