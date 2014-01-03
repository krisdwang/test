package com.amazon.messaging.seqstore.v3.bdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import com.amazon.messaging.impls.AlwaysIncreasingClock;
import com.amazon.messaging.seqstore.SeqStoreTestUtils;
import com.amazon.messaging.seqstore.v3.Entry;
import com.amazon.messaging.seqstore.v3.SeqStore;
import com.amazon.messaging.seqstore.v3.SeqStoreReader;
import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.config.SeqStoreConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStorePersistenceConfig;
import com.amazon.messaging.seqstore.v3.exceptions.InvalidConfigException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;

public class BDBConfigStoreTest {

    private static final StoreId STOREID1 = new StoreIdImpl("BDBConfigTest", "1");

    private static final StoreId STOREID2 = new StoreIdImpl("BDBConfigTest", "2");

    private BDBStoreManager<BasicInflightInfo> manager;

    private final BasicConfigProvider<BasicInflightInfo> configProvider =
    	BasicConfigProvider.newBasicInflightConfigProvider();

    String payload;

    private static final int payloadSize = 2048;

    private void setup() throws IOException, InvalidConfigException, SeqStoreException {
        SeqStorePersistenceConfig con = new SeqStorePersistenceConfig();
        con.setStoreDirectory(SeqStoreTestUtils.setupBDBDir(getClass().getSimpleName()));
        
        manager = BDBStoreManager.createDefaultTestingStoreManager(
                configProvider, con.getImmutableConfig(), new AlwaysIncreasingClock() );  
        basicSetup();
    }

    private void shutDown() throws SeqStoreException {
        manager.close();
    }

    private void basicSetup() {
        StringBuffer payloadB = new StringBuffer();
        for (int i = 0; i < payloadSize; i++) {
            payloadB.append('a');
        }
        payload = payloadB.toString();
    }

    private Entry getMessage(final int i) {
        return new Entry() {

            @Override
            public String getLogId() {
                return Integer.toString(i);
            }

            @Override
            public byte[] getPayload() {
                return payload.getBytes();
            }
        };
    }

    @Test
    public void testCloseStoreThenReopen() throws SeqStoreException, InterruptedException, IOException {
        try {
            setup();
            manager.createStore(STOREID1);
            manager.createStore(STOREID2);
            SeqStore<AckIdV3, BasicInflightInfo> store = manager.getStore(STOREID1);
            assertNotNull(store);
            store.createReader(store.getStoreName() + "reader1");
            SeqStoreReader<AckIdV3, BasicInflightInfo> reader = store.getReader(store.getStoreName() + "reader1");
            assertNotNull(reader);
            for (int i = 0; i < 10; i++) {
                store.enqueue(getMessage(i), -1, null);
            }
            StoredEntry<AckIdV3> message1 = reader.dequeue();
            reader.ack(message1.getAckId());
            assertNotNull(message1);
            assertEquals(message1.getLogId(), "" + 0);
            // the store is closed from the memory
            manager.closeStore(STOREID1);
            // test whether the manager can get the closed store back from the
            // disk
            store = manager.getStore(STOREID1);
            assertNotNull(store);
            // test whether the reader can be restored;
            reader = store.getReader(store.getStoreName() + "reader1");
            StoredEntry<AckIdV3> message2 = reader.dequeue();
            assertEquals(message2.getLogId(), "" + 1);
            manager.closeStore(STOREID1);
            SeqStoreConfig storeConfig = new SeqStoreConfig();
            configProvider.putStoreConfig(STOREID1.getGroupName(), storeConfig);
            // test reconfig store when it's closed
            manager.createStore(STOREID1);
            store = manager.getStore(STOREID1);
            // the config should be updated

            // the message was not acked, it will be redrived
            reader = store.getReader(store.getStoreName() + "reader1");
            assertNotNull(reader);
            StoredEntry<AckIdV3> message3 = reader.dequeue();
            reader.ack(message3.getAckId());

            assertEquals(message3.getLogId(), "" + 1);
            for (int i = 10; i < 20; i++) {
                store.enqueue(getMessage(i), -1, null);
            }

            for (int i = 2; i < 20; i++) {
                StoredEntry<AckIdV3> message = reader.dequeue();
                assertEquals(message.getLogId(), "" + i);
                reader.ack(message.getAckId());

            }
            assertEquals(null, reader.dequeue());

        } finally {
            shutDown();
        }
    }

    @Test
    public void testDeleteStoreThenRecreate() throws SeqStoreException, InterruptedException, IOException, TimeoutException {
        try {
            setup();
            manager.createStore(STOREID1);
            manager.createStore(STOREID2);
            SeqStore<AckIdV3, BasicInflightInfo> store = manager.getStore(STOREID1);
            assertNotNull(store);
            store.createReader(store.getStoreName() + "reader1");
            SeqStoreReader<AckIdV3, BasicInflightInfo> reader = store.getReader(store.getStoreName() + "reader1");
            assertNotNull(reader);
            for (int i = 0; i < 10; i++) {
                store.enqueue(getMessage(i), -1, null);
            }
            StoredEntry<AckIdV3> message1 = reader.dequeue();
            reader.ack(message1.getAckId());
            assertNotNull(message1);
            assertEquals(message1.getLogId(), "" + 0);
            // the store is closed from the memory
            manager.deleteStore(STOREID1);
            // test whether the manager can get the closed store back from the
            // disk
            store = manager.getStore(STOREID1);
            assertEquals(null, store);
            manager.waitForAllStoreDeletesToFinish(1, TimeUnit.SECONDS);
            // test whether the reader can be restored;
            manager.createStore(STOREID1);
            store = manager.getStore(STOREID1);
            store.createReader(store.getStoreName() + "reader1");
            reader = store.getReader(store.getStoreName() + "reader1");
            StoredEntry<AckIdV3> message2 = reader.dequeue();
            assertEquals(null, message2);
            for (int i = 0; i < 10; i++) {
                store.enqueue(getMessage(i), -1, null);
            }
            message2 = reader.dequeue();
            reader.ack(message2.getAckId());
            assertEquals("" + 0, message2.getLogId());

            manager.closeStore(STOREID1);
            SeqStoreConfig storeConfig = new SeqStoreConfig();
            configProvider.putStoreConfig(STOREID1.getGroupName(), storeConfig);
            // test reconfig store when it's closed
            manager.createStore(STOREID1);
            store = manager.getStore(STOREID1);

            // the message was not acked, it will be redrived
            reader = store.getReader(store.getStoreName() + "reader1");
            StoredEntry<AckIdV3> message3 = reader.dequeue();
            reader.ack(message3.getAckId());

            assertEquals(message3.getLogId(), "" + 1);
            for (int i = 10; i < 20; i++) {
                store.enqueue(getMessage(i), -1, null);
            }

            for (int i = 2; i < 20; i++) {
                StoredEntry<AckIdV3> message = reader.dequeue();
                assertEquals(message.getLogId(), "" + i);
                reader.ack(message.getAckId());

            }
            assertEquals(null, reader.dequeue());

        } finally {
            shutDown();
        }
    }
}
