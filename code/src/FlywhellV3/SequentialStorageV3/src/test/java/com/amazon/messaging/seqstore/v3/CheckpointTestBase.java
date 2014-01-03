package com.amazon.messaging.seqstore.v3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.amazon.messaging.impls.SettableClock;
import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.v3.config.ConfigProvider;
import com.amazon.messaging.seqstore.v3.config.SeqStoreReaderConfig;
import com.amazon.messaging.seqstore.v3.exceptions.InvalidConfigException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.SeqStoreManagerV3;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.testing.TestCase;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;
import com.amazon.messaging.util.BasicInflightInfoFactory;
import com.amazon.messaging.utils.Pair;

public abstract class CheckpointTestBase extends TestCase {

    protected abstract SeqStoreManagerV3<BasicInflightInfo> getManager(Clock clock,
                                                                     ConfigProvider<BasicInflightInfo> configProvider,
                                                                     CheckpointProvider<StoreId, AckIdV3, AckIdV3, BasicInflightInfo> checkpointProvider)
            throws InvalidConfigException, SeqStoreException, IOException;

    public static class TestCheckpointProvider implements CheckpointProvider<StoreId, AckIdV3, AckIdV3, BasicInflightInfo> {

        private Map<Pair<StoreId, String>, Checkpoint<AckIdV3, AckIdV3, BasicInflightInfo>> checkpointMap = new HashMap<Pair<StoreId, String>, Checkpoint<AckIdV3, AckIdV3, BasicInflightInfo>>();

        @Override
        public Checkpoint<AckIdV3, AckIdV3, BasicInflightInfo> getCheckpointForReader(StoreId storeId,
                                                                                      String readerName) {
            return checkpointMap.remove(new Pair<StoreId, String>(storeId, readerName));
        }

        @SuppressWarnings("unchecked")
        public void storeCheckpoint(StoreId storeId, String readerName,
                                    Checkpoint<AckIdV3, ?, BasicInflightInfo> checkpoint) {
            checkpointMap.put(
                    new Pair<StoreId, String>(storeId, readerName),
                    (Checkpoint<AckIdV3, AckIdV3, BasicInflightInfo>) checkpoint);
        }

        @Override
        public void readerDeleted(StoreId storeId, String readerName) {
            checkpointMap.remove(new Pair<StoreId, String>(storeId, readerName));
        }
    }
    
    protected void readAndAckMessages(SeqStoreReader<AckIdV3, BasicInflightInfo> reader, int start, int end,
                                    boolean ackOnlyEvenMessages, boolean skipEvenMessages) 
        throws SeqStoreException, InterruptedException
    {
        for (int i = start; i < end; ++i) {
            if( skipEvenMessages && i % 2 == 0 ) continue;
            
            InflightEntry<AckIdV3, BasicInflightInfo> msg = reader.dequeue();
            assertNotNull(msg);
            assertEquals("Test" + i, msg.getLogId());
            if (!ackOnlyEvenMessages || i % 2 == 0)
                reader.ack(msg.getAckId());
        }
    }
                                    

    @Test
    public void testCopyToNewReader()
            throws InvalidConfigException, SeqStoreException, IOException, InterruptedException {
        final int numMessages = 50;
        final int redeliveryTimeout = 4 * 10;

        final StoreId storeId = new StoreIdImpl("store");
        final String reader1Id = "Reader1";
        final String reader2Id = "Reader2";

        SettableClock clock = new SettableClock();
        BasicConfigProvider<BasicInflightInfo> configProvider = BasicConfigProvider.newBasicInflightConfigProvider();
        TestCheckpointProvider checkpointProvider = new TestCheckpointProvider();

        SeqStoreManagerV3<BasicInflightInfo> manager = getManager(
                clock, configProvider, checkpointProvider);

        try {
            SeqStoreReaderConfig<BasicInflightInfo> readerConfig = BasicConfigProvider.newBasicInflightReaderConfig();
            readerConfig.setInflightInfoFactory(new BasicInflightInfoFactory(redeliveryTimeout));
            configProvider.putReaderConfig(storeId.getGroupName(), reader1Id, readerConfig);
            configProvider.putReaderConfig(storeId.getGroupName(), reader2Id, readerConfig);
    
            SeqStore<AckIdV3, BasicInflightInfo> store = manager.createStore(storeId);
            SeqStoreReader<AckIdV3, BasicInflightInfo> reader1 = store.createReader(reader1Id);
    
            for (int i = 0; i < numMessages; ++i) {
                store.enqueue(new TestEntry("Test" + i), 10, null);
            }
    
            // Ack every other of the first 1/4 of the messages
            readAndAckMessages( reader1, 0, numMessages/4, true, false );
    
            clock.increaseTime(redeliveryTimeout / 2);
    
            // Ack every other of the second 1/4 of the messages, but with a later time
            //  so they will only become available for redelivery later
            readAndAckMessages( reader1, numMessages/4, numMessages/2, true, false );
            
            // Store the checkpoint for reader 1 as a checkpoint to use for reader 2 and create reader 2
            checkpointProvider.storeCheckpoint( storeId, reader2Id, reader1.getCheckpoint());
            SeqStoreReader<AckIdV3, BasicInflightInfo> reader2 = store.createReader(reader2Id);
            
            // The metrics should be the same for reader 1 and reader 2
            assertEquals( new InflightMetrics( numMessages / 4, 0 ),  reader1.getInflightMetrics() );
            assertEquals( new InflightMetrics( numMessages / 4, 0 ),  reader2.getInflightMetrics() );
            
            assertEquals( numMessages / 2, reader1.getStoreBacklogMetrics().getQueueDepth() );
            assertEquals( numMessages / 2, reader2.getStoreBacklogMetrics().getQueueDepth() );
            
            // Get and ack the remaining messages for reader 1 and 2
            readAndAckMessages( reader1, numMessages/2, numMessages, false, false );
            readAndAckMessages( reader2, numMessages/2, numMessages, false, false );
            
            // Neither reader should have remaining messages
            assertNull( reader1.dequeue() );
            assertNull( reader2.dequeue() );

            // Increase the time so the first set of timed out messages should become available
            clock.increaseTime(redeliveryTimeout / 2);
            
            // Get and ack the unacked the messages from the first 1/4 
            readAndAckMessages( reader1, 0, numMessages/4, false, true );
            readAndAckMessages( reader2, 0, numMessages/4, false, true );
            
            // Nothing else should be available dequeue until the time advances
            assertNull( reader1.dequeue() );
            assertNull( reader2.dequeue() );
    
            clock.increaseTime(redeliveryTimeout / 2);
    
            // Read and the messages left over from the second 1/4 of the messages
            readAndAckMessages( reader1, numMessages/4, numMessages/2, false, true );
            readAndAckMessages( reader2, numMessages/4, numMessages/2, false, true );
                        
            // Nothing should be left
            assertNull( reader1.dequeue() );
            assertNull( reader2.dequeue() );
        } finally {
            manager.close();
        }
    }
}
