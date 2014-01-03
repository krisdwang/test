package com.amazon.messaging.seqstore.v3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazon.messaging.impls.SettableClock;
import com.amazon.messaging.seqstore.v3.Entry;
import com.amazon.messaging.seqstore.v3.SeqStoreReader;
import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.config.SeqStoreConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.SeqStoreManagerV3;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreInternalInterface;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;

public abstract class MaxRetentionPeriodTestBase {

    protected static final int INTERVAL = 100;

    protected final SettableClock clock = new SettableClock();

    protected final BasicConfigProvider<BasicInflightInfo> confProvider =
    	BasicConfigProvider.newBasicInflightConfigProvider();

    static class TestEntry extends Entry {

        private final String id_;

        private final byte[] payload_;

        public TestEntry(int sequence, int data) {
            id_ = Integer.toString(sequence);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            try {
                dos.writeInt(data);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }

            payload_ = baos.toByteArray();
        }

        @Override
        public String getLogId() {
            return id_;
        }

        @Override
        public byte[] getPayload() {
            return payload_;
        }
    }

    protected SeqStoreInternalInterface<BasicInflightInfo> store_;

    protected SeqStoreManagerV3<BasicInflightInfo> manager_;

    public MaxRetentionPeriodTestBase() {
        super();
    }

    @Before
    public abstract void setUp() throws SeqStoreException, IOException;

    @After
    public abstract void tearDown() throws SeqStoreException;

    @Test
    public void testWithClosedReaders() throws SeqStoreException, Exception {
        runTests();
    }

    @Test
    public void testWithOpenReaders() throws SeqStoreException, Exception {
        @SuppressWarnings("unused")
        SeqStoreReader<AckIdV3, BasicInflightInfo> ssr = store_.getReader("reader-1");
        try {
            runTests();
        } finally {
            store_.removeReader("reader-1");
        }
    }

    private void runTests() throws SeqStoreException, Exception {
        // Make the cleaner run.
        SeqStoreConfig config = new SeqStoreConfig();
        config.setMaxMessageLifeTime(INTERVAL);
        config.setCleanerPeriod(INTERVAL);
        confProvider.putStoreConfig(store_.getStoreId().getGroupName(), config);
        store_.updateConfig();

        for (int i = 0; i < 10; ++i)
            store_.enqueue(new TestEntry(i, i), -1, null);

        clock.setCurrentTime(3 * INTERVAL);
        store_.runCleanupNow();
        confProvider.putReaderConfig(store_.getStoreName(), "newreader-3",
        		BasicConfigProvider.newBasicInflightReaderConfig());
        store_.createReader("newreader-3");
        SeqStoreReader<AckIdV3, BasicInflightInfo> ssr = store_.getReader("newreader-3");

        // we dont delete msgs that last available lies in
        try {
            assertEquals(null, ssr.dequeue());
        } finally {
            store_.removeReader("newreader-3");
        }
    }

    @Test
    public void testAckExpiredMessage() throws SeqStoreException, InterruptedException {
        // Enqueue 2 messages
        for (int i = 0; i < 2; ++i)
            store_.enqueue(new TestEntry(i, i), -1, null);

        // Dequeue those messages, but don't ack just yet.
        SeqStoreReader<AckIdV3, BasicInflightInfo> ssr = store_.getReader("reader-1");
        try {
            StoredEntry<AckIdV3> first = ssr.dequeue();
            assertNotNull(first);
            StoredEntry<AckIdV3> second = ssr.dequeue();
            assertNotNull(second);

            Thread.sleep(2 * INTERVAL);

            // Ack the messages.
            ssr.ack(first.getAckId());
            ssr.ack(second.getAckId());
        } finally {
            store_.removeReader("reader-1");
        }
    }

    @Test
    public void testNackExpiredMessage() throws SeqStoreException, InterruptedException {
        // Enqueue 2 messages
        for (int i = 0; i < 2; ++i)
            store_.enqueue(new TestEntry(i, i), -1, null);

        // Dequeue those messages, but don't ack just yet.
        SeqStoreReader<AckIdV3, BasicInflightInfo> ssr = store_.getReader("reader-1");
        try {
            StoredEntry<AckIdV3> first = ssr.dequeue();
            assertNotNull(first);
            StoredEntry<AckIdV3> second = ssr.dequeue();
            assertNotNull(second);

            Thread.sleep(2 * INTERVAL);

            // nack the messages
            ssr.update(first.getAckId(), new InflightUpdateRequest<BasicInflightInfo>().withNewTimeout(0) );
            ssr.update(second.getAckId(), new InflightUpdateRequest<BasicInflightInfo>().withNewTimeout(0) );
        } finally {
            store_.removeReader("reader-1");
        }
    }

    @Test
    public void testDequeueExpiredNacked() throws SeqStoreException, InterruptedException {
        // Enqueue 2 messages
        for (int i = 0; i < 2; ++i)
            store_.enqueue(new TestEntry(i, i), -1, null);

        // Dequeue and nack
        SeqStoreReader<AckIdV3, BasicInflightInfo> ssr = store_.getReader("reader-1");
        try {
            StoredEntry<AckIdV3> first = ssr.dequeue();
            assertNotNull(first);
            StoredEntry<AckIdV3> second = ssr.dequeue();
            assertNotNull(second);

            ssr.update(first.getAckId(), new InflightUpdateRequest<BasicInflightInfo>().withNewTimeout(0) );
            ssr.update(second.getAckId(), new InflightUpdateRequest<BasicInflightInfo>().withNewTimeout(0) );

            // Make the cleaner run.
            SeqStoreConfig config = new SeqStoreConfig();
            config.setMaxMessageLifeTime(INTERVAL);
            config.setCleanerPeriod(INTERVAL);
            confProvider.putStoreConfig(store_.getStoreId().getGroupName(), config);
            store_.updateConfig();
            store_.runCleanupNow();

            clock.setCurrentTime(3 * INTERVAL);
            Thread.sleep(2 * INTERVAL);
            confProvider.putReaderConfig(store_.getStoreName(), "reader-2",
            		BasicConfigProvider.newBasicInflightReaderConfig());
            store_.createReader("reader-2");
            SeqStoreReader<AckIdV3, BasicInflightInfo> ssr2 = store_.getReader("reader-2");
            // Dequeue should return no messages
            first = ssr2.dequeue();
            assertNull(first);
        } finally {
            store_.removeReader("reader-1");
            store_.removeReader("reader-2");
        }
    }
    /*
     * @Test public void testCallback() throws SeqStoreException,
     * InterruptedException { final String d[] = new String[1]; final String r[]
     * = new String[1]; final long n[] = new long[1];
     * manager_.setMessageExpiryListener(new MessageExpiryListener() { public
     * void notifyMessagesExpired(String dest, String reader, long number) {
     * d[0] = dest; r[0] = reader; n[0] = number; } }); for (int i = 0; i < 2;
     * ++i) store_.enqueue(new TestEntry(i, i)); Thread.sleep(7 * 1000); //
     * Callback should have been called assertNotNull(d[0]); assertEquals("foo",
     * d[0]); assertNotNull(r[0]); assertEquals("reader-1", r[0]);
     * assertEquals(2, n[0]); }
     */

}
