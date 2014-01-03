package com.amazon.messaging.seqstore.v3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import com.amazon.messaging.seqstore.v3.Entry;
import com.amazon.messaging.seqstore.v3.InflightMetrics;
import com.amazon.messaging.seqstore.v3.SeqStore;
import com.amazon.messaging.seqstore.v3.SeqStoreReader;
import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.config.SeqStoreConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStoreReaderConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.StoreIdImpl;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreManager;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.seqstore.v3.mapPersistence.DelayedQV3Test;
import com.amazon.messaging.testing.TestCase;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;

public abstract class DelayedQV3TestBase extends TestCase {

    private static final class TestEntry extends Entry {

        private final String logId;

        private final long delay;

        private final long startTime;

        private TestEntry(String logId, long delay, long startTime) {
            this.logId = logId;
            this.delay = delay;
            this.startTime = startTime;
        }

        @Override
        public String getLogId() {
        	return logId;
        }

        @Override
        public byte[] getPayload() {
        	return PAYLOAD_BYTES;
        }

        @Override
        public long getAvailableTime() {
            return startTime + delay;
        }
    }

    protected static final Log log = LogFactory.getLog(DelayedQV3Test.class);

    static final byte[] PAYLOAD_BYTES = "Payload".getBytes();

    protected static final StoreId DESTINATION = new StoreIdImpl("delayedQ");

	protected static final String READER = "delayedQReader";

	protected final BasicConfigProvider<BasicInflightInfo> configProvider =
		BasicConfigProvider.newBasicInflightConfigProvider();

    protected SeqStore<AckIdV3, BasicInflightInfo> store_;

    protected SeqStoreManager<AckIdV3, BasicInflightInfo> manager_;

	protected final AtomicInteger msgCounter_ = new AtomicInteger(0);

	protected final AtomicInteger threadCounter_ = new AtomicInteger(0);

	protected Throwable error = null;

	public DelayedQV3TestBase() {
		super();
	}

	public abstract void setUp() throws SeqStoreException, IOException;

	public abstract void tearDown() throws SeqStoreException;

	@Test
	public void testMultiThreadedDequeuingWithDelayedAndAvailableMessageBacklog()
			throws Exception {
		setUp();
		try {
			SeqStoreConfig storeConfig = new SeqStoreConfig();
			storeConfig.setGuaranteedRetentionPeriodSeconds(30);
			storeConfig.setCleanerPeriod(1000);
            configProvider.putStoreConfig(DESTINATION.getGroupName(), storeConfig);
			SeqStoreReaderConfig<BasicInflightInfo> readerConfig = BasicConfigProvider.newBasicInflightReaderConfig();
            configProvider.putReaderConfig(DESTINATION.getGroupName(), READER, readerConfig);
			final long delay = 10000;
			final int numThreads = 101;

			manager_.createStore(DESTINATION);
			store_ = manager_.getStore(DESTINATION);
			store_.createReader(READER);
			final SeqStoreReader<AckIdV3, BasicInflightInfo> reader = store_.getReader(READER);

			final Random randomNumGen = new Random();
			final long startTime = System.currentTimeMillis();
			// create a delayed backlog of size numThreads
			for (int i = 0; i < numThreads; i++) {
				final String logId = "Metadata-" + i;
				Entry e = new TestEntry(logId, delay, startTime);
				store_.enqueue(e, -1, null);
			}
			// spawn off numThreads each of which keep attempting to dequeue
			// messages until 10 seconds before finalEndTime when all of the 
			// delayed messages in the backlog are available. All of these 
			// dequeue attempts should return null.
			final long finalEndTime = System.currentTimeMillis() + delay;
			final long endTime = startTime + delay - 7000;
			error = null;
			Thread[] dequeuers = new Thread[numThreads];
			final CyclicBarrier barrier = new CyclicBarrier(dequeuers.length);
			for (int i = 0; i < dequeuers.length; i++) {
				dequeuers[i] = new Thread() {

					@Override
					public void run() {
						try {
							barrier.await();
							while (System.currentTimeMillis() < endTime) {
								assertNull(reader.dequeue());
								Thread.sleep(randomNumGen.nextInt(100));
							}
						} catch (Throwable e) {
							e.printStackTrace();
							error = e;
							throw new IllegalStateException(e);
						}
					}
				};
				dequeuers[i].start();
			}
            for (Thread dequeuer : dequeuers) {
                dequeuer.join();
			}
			assertNull(error);
            assertEquals(numThreads, reader.getStoreBacklogMetrics().getNumDelayed());
            assertEquals(0, reader.getInflightMetrics().getNumInFlight());
			// sleep till finalEndTime after which all delayed messages in the
			// backlog are available for dequeuing
			// make sure last available is called by cleaner at least once
			Thread.sleep(finalEndTime - System.currentTimeMillis() + 2000);
			
			assertEquals(numThreads, reader.getStoreBacklogMetrics().getQueueDepth());
			
			// spawn off numThreads that attempt to dequeue messages, since there 
			// is an available  backlog every dequeue call should return a non null value 
			// till the queue depth becomes 0
			for (int i = 0; i < dequeuers.length; i++) {
				dequeuers[i] = new Thread() {

					protected int id_ = threadCounter_.incrementAndGet();

					@Override
					public void run() {
						try {
							barrier.await();
							while (true) {
								StoredEntry<AckIdV3> e = reader.dequeue();
								if (e == null) {
									break;
								} 
								System.out.println("Thread " + id_ + " dequeued message: " + e.getLogId()
								        + " ackId.SeqNum: ");
								reader.ack(e.getAckId());

								Thread.yield();
							}
						} catch (Throwable e) {
							e.printStackTrace();
							error = e;
							throw new IllegalStateException(e);
						}
					}
				};
				dequeuers[i].start();
			}
            for (Thread dequeuer : dequeuers) {
                dequeuer.join();
			}
			 Thread.sleep(3000);
            
			assertNull(error);
            assertEquals(InflightMetrics.ZeroInFlight, reader.getInflightMetrics() );
            assertEquals(0l, reader.getStoreBacklogMetrics().getQueueDepth());
            assertEquals(0l, reader.getStoreBacklogMetrics().getNumDelayed());
			store_.removeReader(READER);
		} finally {
			tearDown();
		}
	}

	@Test
	public void testMultiThreadedDequeuingWithNoBackLog() throws Exception {
		setUp();
		try {
			SeqStoreConfig storeConfig = new SeqStoreConfig();
			storeConfig.setGuaranteedRetentionPeriodSeconds(30);
            configProvider.putStoreConfig(DESTINATION.getGroupName(), storeConfig);
			SeqStoreReaderConfig<BasicInflightInfo> readerConfig = BasicConfigProvider.newBasicInflightReaderConfig();
            configProvider.putReaderConfig(DESTINATION.getGroupName(), READER, readerConfig);
			final long delay = 2000;
			final int numThreads = 10;

			manager_.createStore(DESTINATION);
			store_ = manager_.getStore(DESTINATION);
			store_.createReader(READER);
			final SeqStoreReader<AckIdV3, BasicInflightInfo> reader = store_.getReader(READER);

			final long startTime = System.currentTimeMillis();
			final long endTime = startTime + 10000;
			Thread[] enqueuersDequeuers = new Thread[numThreads];
            final CyclicBarrier barrier = new CyclicBarrier(enqueuersDequeuers.length);

			for (int i = 0; i < enqueuersDequeuers.length; i++) {
				enqueuersDequeuers[i] = new Thread() {

					protected int id = threadCounter_.incrementAndGet();

					@Override
					public void run() {
						try {
							barrier.await();
							while (System.currentTimeMillis() < endTime) {
                                final String logId = "Metadata-" + msgCounter_.incrementAndGet();
								Entry e = new Entry() {

									@Override
									public String getLogId() {
										return logId;
									}

									@Override
									public byte[] getPayload() {
										return PAYLOAD_BYTES;
									}
								};
								store_.enqueue(e, -1, null);
                                log.debug("Thread " + id + " enqueued message: " + logId);
                                long msgAvailableTime = System.currentTimeMillis() + delay;
                                StoredEntry<AckIdV3> se = reader.dequeue();
								if (se != null) {
                                    log.debug("Thread " + id + " dequeued message: " + se.getLogId()
													+ " ackId.SeqNum: ");
									reader.ack(se.getAckId());
								}
                                long sleepTime = msgAvailableTime - System.currentTimeMillis();
								if (sleepTime > 0) {
									Thread.sleep(sleepTime);
								}
                                assertTrue(msgAvailableTime <= System.currentTimeMillis());
								se = reader.dequeue();
								if (se != null) {
									reader.ack(se.getAckId());
                                    log.debug("Thread " + id + " dequeued message: " + se.getLogId()
													+ " ackId.SeqNum: ");
								} else {
                                    log.debug("Thread " + id + " dequeue returned null ");
								}
							}
						} catch (Throwable e) {
							e.printStackTrace();
							error = e;
						}
					}
				};
				enqueuersDequeuers[i].start();
			}

            for (Thread enqueuersDequeuer : enqueuersDequeuers) {
                enqueuersDequeuer.join();
			}
			assertNull(error);
			// since null can be returned even if there are messages available
			// to be
			// dequeued get the remaining messages
            long remainingMsgs = reader.getStoreBacklogMetrics().getQueueDepth() +
                reader.getInflightMetrics().getNumAvailableForRedelivery();
            
            System.out.println("Threads concurrently enqueued " + msgCounter_.get() + " and dequeued  "
					+ (msgCounter_.get() - remainingMsgs) + " messages");

			assertEquals(0l, remainingMsgs);          
            assertEquals(InflightMetrics.ZeroInFlight, reader.getInflightMetrics());
            assertEquals(0l, reader.getStoreBacklogMetrics().getNumDelayed());
            assertEquals(0l, reader.getStoreBacklogMetrics().getQueueDepth());
			store_.removeReader(READER);
            manager_.closeStore(DESTINATION);
		} finally {
			tearDown();
		}

	}

}
