package com.amazon.messaging.seqstore.v3.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.amazon.messaging.impls.SettableClock;
import com.amazon.messaging.seqstore.v3.InflightEntry;
import com.amazon.messaging.seqstore.v3.InflightUpdateRequest;
import com.amazon.messaging.seqstore.v3.InflightUpdateResult;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreReaderV3InternalInterface;
import com.amazon.messaging.util.BasicInflightInfo;

public abstract class TimeoutCapableMessageSourceTestBase {

    protected SeqStoreReaderV3InternalInterface<BasicInflightInfo> source;

    protected SettableClock clock_ = new SettableClock();

    protected static final int INTERVAL = 300;
    
    public static void assertNotEquals(Object unexpected, Object got) {
        if( ( unexpected == got ) || ( unexpected != null && unexpected.equals( got ) ) ) {
            fail("Expected not: <" + unexpected + "> but got it anyway.");
        }
    }

    public static void assertNotEquals(String msg, Object unexpected, Object got) {
        if( ( unexpected == got ) || ( unexpected != null && unexpected.equals( got ) ) ) {
            fail( msg + ": " + "Expected not: <" + unexpected + "> but got it anyway." );
        }
    }

    public abstract void setUp() throws Exception;

    public abstract void tearDown() throws Exception;

    @Test
    public void testTimeout() throws InterruptedException, SeqStoreException {
        InflightEntry<AckIdV3, BasicInflightInfo> m1 = source.dequeue();
        InflightEntry<AckIdV3, BasicInflightInfo> m2 = source.dequeue();
        assertNotEquals(m1.getAckId(), m2.getAckId());
        assertEquals( 1, m1.getInflightInfo().getDeliveryCount() );
        assertEquals( 1, m2.getInflightInfo().getDeliveryCount() );
        assertEquals( INTERVAL, m1.getDelayUntilNextRedrive() );
        assertEquals( INTERVAL, m2.getDelayUntilNextRedrive() );
        
        clock_.setCurrentTime(clock_.getCurrentTime() + INTERVAL);
        
        // should be m1 redriven
        InflightEntry<AckIdV3, BasicInflightInfo> m1b = source.dequeue();
        // should be m2 redriven
        InflightEntry<AckIdV3, BasicInflightInfo> m2b = source.dequeue();
        assertEquals(m1.getStoredEntry(), m1b.getStoredEntry());
        assertEquals(m2.getStoredEntry(), m2b.getStoredEntry());
        assertEquals( 2, m1b.getInflightInfo().getDeliveryCount() );
        assertEquals( 2, m2b.getInflightInfo().getDeliveryCount() );
    }

    @Test
    public void testExtendTimeout() throws InterruptedException, SeqStoreException {
        InflightEntry<AckIdV3, BasicInflightInfo> m1 = source.dequeue();
        InflightEntry<AckIdV3, BasicInflightInfo> m2 = source.dequeue();
        assertNotEquals(m1.getAckId(), m2.getAckId());
        extendTimeout(m1.getAckId(), 2 * INTERVAL);
        
        InflightEntry<AckIdV3, BasicInflightInfo> m1Inflight = source.getInFlightMessage( m1.getAckId() );
        assertNotNull(m1Inflight);
        assertEquals(m1Inflight.getAckId(), m1.getAckId());
        assertEquals(2 * INTERVAL, m1Inflight.getDelayUntilNextRedrive());
        
        clock_.setCurrentTime(clock_.getCurrentTime() + INTERVAL);
        
        // should be m2 redriven
        InflightEntry<AckIdV3, BasicInflightInfo> m2b = source.dequeue();
        assertEquals(m2.getAckId(), m2b.getAckId());
        
        // new message.
        InflightEntry<AckIdV3, BasicInflightInfo> m3 = source.dequeue();
        assertNotEquals(m3.getAckId(), m1.getAckId());
        assertNotEquals(m3.getAckId(), m2.getAckId());
        
        clock_.setCurrentTime(clock_.getCurrentTime() + INTERVAL);
        
        // should be m1 redriven
        InflightEntry<AckIdV3, BasicInflightInfo> m1b = source.dequeue();
        assertEquals(m1.getAckId(), m1b.getAckId());
    }

    @Test
    public void test() throws InterruptedException, SeqStoreException {
        InflightEntry<AckIdV3, BasicInflightInfo> m1 = source.dequeue();
        InflightEntry<AckIdV3, BasicInflightInfo> m2 = source.dequeue();
        InflightEntry<AckIdV3, BasicInflightInfo> m3 = source.dequeue();
        InflightEntry<AckIdV3, BasicInflightInfo> m4 = source.dequeue();
        InflightEntry<AckIdV3, BasicInflightInfo> m5 = source.dequeue();
        
        assertEquals( INTERVAL, m1.getDelayUntilNextRedrive() );
        
        nack(m1.getAckId());
        ack(m3.getAckId());
        ack(m4.getAckId());
        
        clock_.setCurrentTime(clock_.getCurrentTime() + INTERVAL); 
        AckIdV3 ackLevel = source.getAckLevel();
        assertEquals( ackLevel, new AckIdV3( m1.getAckId(), false ) );

        // should be redriven in nacked order...
        InflightEntry<AckIdV3, BasicInflightInfo> m1b = source.dequeue();
        InflightEntry<AckIdV3, BasicInflightInfo> m2b = source.dequeue();
        InflightEntry<AckIdV3, BasicInflightInfo> m5b = source.dequeue();
        
        assertEquals(m1.getAckId(), m1b.getAckId());
        assertEquals(m2.getAckId(), m2b.getAckId());
        assertEquals(m5.getAckId(), m5b.getAckId());
    }

    public TimeoutCapableMessageSourceTestBase() {
        super();
    }
    
    private boolean ack(AckIdV3 ackId) throws SeqStoreException {
        return source.ack(ackId);
    }
    
    private boolean nack(AckIdV3 ackId) throws SeqStoreException {
        return extendTimeout(ackId, 0 );
    }
    
    private boolean extendTimeout(AckIdV3 ackId, int timeout) throws SeqStoreException {
        return source.update( ackId, new InflightUpdateRequest<BasicInflightInfo>().withNewTimeout(timeout) ) == InflightUpdateResult.DONE;
    }
}
