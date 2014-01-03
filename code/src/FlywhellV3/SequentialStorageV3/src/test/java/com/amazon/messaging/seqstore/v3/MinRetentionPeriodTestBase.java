package com.amazon.messaging.seqstore.v3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.amazon.messaging.impls.SettableClock;
import com.amazon.messaging.seqstore.v3.Entry;
import com.amazon.messaging.seqstore.v3.SeqStoreReader;
import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.config.SeqStoreConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.SeqStoreManagerV3;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreInternalInterface;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.testing.TestCase;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;

public abstract class MinRetentionPeriodTestBase extends TestCase {

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

    protected final SettableClock clock_ = new SettableClock();

    private static final int CLEANUP_INTERVAL = 100;

    private static final int INFINITE_MIN_RETENTION = -1;

    private static final int ZERO_MIN_RETENTION = 0;

    private static final int POSITIVE_MIN_RETENTION = 2 * (CLEANUP_INTERVAL);

    private static final StoreId STOREID = new StoreIdImpl("MinRetentionStore");

    protected SeqStoreInternalInterface<BasicInflightInfo> store_;

    protected abstract SeqStoreManagerV3<BasicInflightInfo> getManager();

    protected BasicConfigProvider<BasicInflightInfo> confProvider =
    	BasicConfigProvider.newBasicInflightConfigProvider();

    private int minRetention_;

    public MinRetentionPeriodTestBase() {
        super();
    }

    public abstract void setUp() throws SeqStoreException, IOException;

    public abstract void tearDown() throws SeqStoreException;

    @Test
    public void testWithInfiniteMinRetention() throws Exception {
        try {
            minRetention_ = INFINITE_MIN_RETENTION;
            withNoReaders();
            getManager().waitForAllStoreDeletesToFinish(1, TimeUnit.SECONDS);
            withClosedReaders();
            getManager().waitForAllStoreDeletesToFinish(1, TimeUnit.SECONDS);
            withOpenReaders();
        } catch (Exception e) {
            throw e;
        } finally {
            // store_.close();
            tearDown();
        }
    }

    @Test
    public void testWithZeroMinRetention() throws Exception {
        try {
            minRetention_ = ZERO_MIN_RETENTION;
            withNoReaders();
            getManager().waitForAllStoreDeletesToFinish(1, TimeUnit.SECONDS);
            withClosedReaders();
            getManager().waitForAllStoreDeletesToFinish(1, TimeUnit.SECONDS);
            withOpenReaders();
        } catch (Exception e) {
            throw e;
        } finally {
            // store_.close();
            tearDown();
        }
    }

    @Test
    public void testWithPositiveMinRetention() throws Exception {
        try {
            minRetention_ = POSITIVE_MIN_RETENTION;
            withNoReaders();
            getManager().waitForAllStoreDeletesToFinish(1, TimeUnit.SECONDS);
            withClosedReaders();
            getManager().waitForAllStoreDeletesToFinish(1, TimeUnit.SECONDS);
            withOpenReaders();
        } catch (Exception e) {
            throw e;
        } finally {
            // store_.close();
            tearDown();
        }
    }

    private void withNoReaders() throws Exception {

        store_ = configureAndGetStore();
        // Enqueue 10 messages
        for (int i = 0; i < 10; ++i)
            store_.enqueue(new TestEntry(i, i), -1, null);

        // wait for cleanup thread to execute before the min retention
        // period has expired.
        clock_.setCurrentTime(clock_.getCurrentTime() + 2 * CLEANUP_INTERVAL);
        store_.runCleanupNow();

        // all of the messages should still be present as long as min
        // retention period is not 0.
        if (minRetention_ != 0) {
            verifyNumMessagesEquals(10);
        } else { // if min retention period is 0 then all messages should be
                 // cleanedup
            verifyNumMessagesEquals(0);
            getManager().deleteStore(STOREID);
            return;
        }

        clock_.setCurrentTime(clock_.getCurrentTime() + 2 * CLEANUP_INTERVAL);
        store_.runCleanupNow();

        if (minRetention_ >= 0) {
            // since the no reader was configured on the destination, the
            // store should have cleanedup all of the messages
            verifyNumMessagesEquals(0);
        }

        if (minRetention_ < 0) {
            // since the min retention is infinite, even if no reader was
            // configured on the
            // destination, all messages in the store should still be present
            verifyNumMessagesEquals(10);
        }
        getManager().deleteStore(STOREID);

    }

    private void withClosedReaders() throws Exception {

        store_ = configureAndGetStore();
        configureAndGetReader(store_, "reader-1");
        verifyCorrectMinRetPolicyEnforcement();
        store_.removeReader("reader-1");
        getManager().deleteStore(STOREID);
    }

    private void withOpenReaders() throws Exception {
        store_ = configureAndGetStore();
        configureAndGetReader(store_, "reader-1");
        @SuppressWarnings("unused")
        SeqStoreReader<AckIdV3, BasicInflightInfo> ssr = store_.getReader("reader-1");
        try {
            verifyCorrectMinRetPolicyEnforcement();
        } finally {
            store_.removeReader("reader-1");
        }
        getManager().deleteStore(STOREID);

    }

    private void verifyCorrectMinRetPolicyEnforcement() throws Exception {
        // Enqueue 10 messages
        for (int i = 0; i < 10; ++i)
            store_.enqueue(new TestEntry(i, i), -1, null);

        clock_.setCurrentTime(clock_.getCurrentTime() + 2 * CLEANUP_INTERVAL);
        store_.runCleanupNow();

        // Since readers were configured before the messages were enqueued
        // none of the enqueued messages should have been cleaned up.
        SeqStoreReader<AckIdV3, BasicInflightInfo> ssr = store_.getReader("reader-1");

        for (int i = 0; i < 10; ++i) {
            StoredEntry<AckIdV3> e = ssr.dequeue();
            assertNotNull(e);
            ssr.ack(e.getAckId());
        }

        clock_.setCurrentTime(clock_.getCurrentTime() + 2 * CLEANUP_INTERVAL);
        store_.runCleanupNow();

        if (minRetention_ >= 0) {
            // since the only reader configured on the destination consumed and
            // acked all of the messages, the store should have cleaned up all
            // of the messages after the min retention period expired
            verifyNumMessagesEquals(0);
        } else {
            // since the min retention period is infinity and max retention
            // period is
            // also infinity none of the messages should be cleaned up even if
            // they have been consumed and acked
            verifyNumMessagesEquals(10);
        }
    }

	private void verifyNumMessagesEquals(long num) throws Exception {
	    configureAndGetReader(store_, "reader-verify");
		SeqStoreReader<AckIdV3, BasicInflightInfo> ssr = store_.getReader("reader-verify");
		try {
		    long actual = ssr.getStoreBacklogMetrics().getQueueDepth() + ssr.getInflightMetrics().getNumAvailableForRedelivery(); 
			assertEquals(num, actual);
		} finally {
			store_.removeReader("reader-verify");
		}
		
	}

    private SeqStoreReader<AckIdV3, BasicInflightInfo> configureAndGetReader(SeqStoreInternalInterface<BasicInflightInfo> store, String readerName) throws SeqStoreException {
        confProvider.putReaderConfig(store.getConfigKey(), readerName,
        		BasicConfigProvider.newBasicInflightReaderConfig());
        return store.createReader(readerName);
    }

    private SeqStoreInternalInterface<BasicInflightInfo> configureAndGetStore() throws Exception {
        SeqStoreConfig storeConfig = new SeqStoreConfig();
        storeConfig.setGuaranteedRetentionPeriod(minRetention_);
        storeConfig.setCleanerPeriod(Long.MAX_VALUE);
        confProvider.putStoreConfig(STOREID.getGroupName(), storeConfig);
        getManager().createStore(STOREID);
        return getManager().getStore(STOREID);
    }

}
