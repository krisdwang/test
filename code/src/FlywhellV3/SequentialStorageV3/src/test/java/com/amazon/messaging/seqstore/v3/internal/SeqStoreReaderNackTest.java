package com.amazon.messaging.seqstore.v3.internal;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazon.messaging.impls.SettableClock;
import com.amazon.messaging.seqstore.v3.InflightEntry;
import com.amazon.messaging.seqstore.v3.InflightUpdateRequest;
import com.amazon.messaging.seqstore.v3.InflightUpdateResult;
import com.amazon.messaging.seqstore.v3.SeqStore;
import com.amazon.messaging.seqstore.v3.TestEntry;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreManager;
import com.amazon.messaging.seqstore.v3.mapPersistence.MapStoreManager;
import com.amazon.messaging.testing.TestCase;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;

/**
 * Based on com.amazon.messaging.seqstore.impl.NackHandlingStoreReaderTest.java
 * Changed to use the v3 classes.
 * 
 * @author robburke
 */
public class SeqStoreReaderNackTest extends TestCase {

    TopDisposableReader<BasicInflightInfo> source = null;

    SeqStoreManager<AckIdV3, BasicInflightInfo> manager = null;

    @Before
    public void setUp() throws Exception {
        SettableClock clock = new SettableClock();
        clock.setCurrentTime(1);
        manager = new MapStoreManager<BasicInflightInfo>(
                BasicConfigProvider.newBasicInflightConfigProvider(), clock);

        SeqStore<AckIdV3, BasicInflightInfo> store = manager.createStore(new StoreIdImpl("store"));
        for (int i = 0; i < 15; i++) {
            TestEntry entry = new TestEntry("message".getBytes(), "" + (i + 1));
            store.enqueue(entry, -1, null);
        }
        store.createReader("reader");
        source = (TopDisposableReader<BasicInflightInfo>) store.getReader("reader");
    }

    @After
    public void tearDown() throws Exception {
        manager.close();
    }

    @Test
    public void testSimple() throws InterruptedException, SeqStoreException {
        InflightEntry<AckIdV3, BasicInflightInfo> m1 = source.dequeue();
        assertEquals("1", m1.getLogId());
        InflightEntry<AckIdV3, BasicInflightInfo> m2 = source.dequeue();
        assertEquals("2", m2.getLogId());
        InflightEntry<AckIdV3, BasicInflightInfo> m3 = source.dequeue();
        assertEquals("3", m3.getLogId());
        InflightEntry<AckIdV3, BasicInflightInfo> m4 = source.dequeue();
        assertEquals("4", m4.getLogId());
        InflightEntry<AckIdV3, BasicInflightInfo> m5 = source.dequeue();
        assertEquals("5", m5.getLogId());
        nack(m1.getAckId());
        ack(m3.getAckId());
        ack(m4.getAckId());
        InflightEntry<AckIdV3, BasicInflightInfo> m1_redrive = source.dequeue();
        assertEquals(m1.getStoredEntry(), m1_redrive.getStoredEntry());
        InflightEntry<AckIdV3, BasicInflightInfo> m6 = source.dequeue();
        assertEquals("6", m6.getLogId());
        InflightEntry<AckIdV3, BasicInflightInfo> m7 = source.dequeue();
        assertEquals("7", m7.getLogId());
        InflightEntry<AckIdV3, BasicInflightInfo> m8 = source.dequeue();
        assertEquals("8", m8.getLogId());
        InflightEntry<AckIdV3, BasicInflightInfo> m9 = source.dequeue();
        assertEquals("9", m9.getLogId());
        InflightEntry<AckIdV3, BasicInflightInfo> m10 = source.dequeue();
        assertEquals("10", m10.getLogId());
        nack(m1.getAckId());
        InflightEntry<AckIdV3, BasicInflightInfo> m1_redrive2 = source.dequeue();
        assertEquals(m1.getStoredEntry(), m1_redrive2.getStoredEntry());
        ack(m9.getAckId());
        ack(m7.getAckId());
        nack(m8.getAckId());
        nack(m5.getAckId());
        InflightEntry<AckIdV3, BasicInflightInfo> m5_redrive1 = source.dequeue();
        assertEquals(m5.getStoredEntry(), m5_redrive1.getStoredEntry());
        InflightEntry<AckIdV3, BasicInflightInfo> m8_redrive1 = source.dequeue();
        assertEquals(m8.getStoredEntry(), m8_redrive1.getStoredEntry());
        ack(m1.getAckId());
        ack(m2.getAckId());
        nack(m8.getAckId());
        InflightEntry<AckIdV3, BasicInflightInfo> m8_redrive2 = source.dequeue();
        assertEquals(m8.getStoredEntry(), m8_redrive2.getStoredEntry());
        InflightEntry<AckIdV3, BasicInflightInfo> m11 = source.dequeue();
        assertEquals("11", m11.getLogId());
        InflightEntry<AckIdV3, BasicInflightInfo> m12 = source.dequeue();
        assertEquals("12", m12.getLogId());
        InflightEntry<AckIdV3, BasicInflightInfo> m13 = source.dequeue();
        assertEquals("13", m13.getLogId());
        nack(m5.getAckId());
        InflightEntry<AckIdV3, BasicInflightInfo> m5_redrive2 = source.dequeue();
        assertEquals(m5.getStoredEntry(), m5_redrive2.getStoredEntry());
        InflightEntry<AckIdV3, BasicInflightInfo> m14 = source.dequeue();
        assertEquals("14", m14.getLogId());
    }

    private boolean ack(AckIdV3 ackId) throws SeqStoreException {
        return source.ack(ackId);
    }
    
    private boolean nack(AckIdV3 ackId) throws SeqStoreException {
        return source.update( ackId, new InflightUpdateRequest<BasicInflightInfo>().withNewTimeout(0) ) == InflightUpdateResult.DONE;
    }
}
