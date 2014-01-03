package com.amazon.messaging.seqstore.v3.internal;

import static org.junit.Assert.*;

import java.util.LinkedHashSet;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.CyclicBarrier;

import org.junit.Test;

import com.amazon.messaging.impls.AlwaysIncreasingClock;
import com.amazon.messaging.impls.SettableClock;
import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.v3.Entry;
import com.amazon.messaging.seqstore.v3.InflightEntry;
import com.amazon.messaging.seqstore.v3.InflightEntryInfo;
import com.amazon.messaging.seqstore.v3.InflightMetrics;
import com.amazon.messaging.seqstore.v3.InflightUpdateRequest;
import com.amazon.messaging.seqstore.v3.InflightUpdateResult;
import com.amazon.messaging.seqstore.v3.SeqStoreReaderMetrics;
import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.TestEntry;
import com.amazon.messaging.seqstore.v3.config.SeqStoreReaderConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreInternalInterface;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreReaderV3InternalInterface;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.testing.TestCase;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;
import com.amazon.messaging.util.BasicInflightInfoFactory;

public abstract class SeqStoreReaderV3TestBase extends TestCase {

    protected static final StoreId STOREID = new StoreIdImpl("store");
    
    protected static final int DELIVERY_ATTEMPTS = 2;

    protected SeqStoreInternalInterface<BasicInflightInfo> store;

	protected SeqStoreReaderV3InternalInterface<BasicInflightInfo> source;

	protected AckIdGenerator ackGen_ = new AckIdGenerator();
	
	Throwable error = null;


	public abstract void setUp(Clock clock, SeqStoreReaderConfig<BasicInflightInfo> config) throws Exception;
	public abstract void tearDown() throws Exception;
	
	public void setUp(Clock clock) throws Exception {
	    setUp(clock, BasicConfigProvider.newBasicInflightReaderConfig());
	}

    protected void loadStore(long num) throws SeqStoreException, InterruptedException {
		for (long i = 0; i < num; i++) {
            Entry entry = new TestEntry("message - " + i, "" + i);
			store.enqueue(entry, -1, null);
		}
	}

	@Test
	public void testMultiThreadedAckingAndNacking() throws Throwable {
		setUp(new AlwaysIncreasingClock());
		Thread[] ackers = new Thread[2];
		final long batchesPerThread = 23;
		final int messagesPerBatch = 67;
		
		loadStore(ackers.length * batchesPerThread * messagesPerBatch);
		
		SeqStoreReaderMetrics metrics = source.getStoreBacklogMetrics();
		InflightMetrics inflightMetrics = source.getInflightMetrics();
        assertEquals(ackers.length * batchesPerThread * messagesPerBatch, metrics.getQueueDepth());
		assertEquals(ackers.length * batchesPerThread * messagesPerBatch,
				metrics.getQueueDepth() + inflightMetrics.getNumAvailableForRedelivery() );
		assertEquals(0l, metrics.getNumDelayed());
		
		final CyclicBarrier barrier = new CyclicBarrier(ackers.length);
		for (int acker = 0; acker < ackers.length; acker++) {
			ackers[acker] = new Thread() {

				@SuppressWarnings("unchecked")
                @Override
				public void run() {
					StoredEntry<AckIdV3>[] m = new StoredEntry[messagesPerBatch];
					try {
						barrier.await();
						for (long i = 0; i < batchesPerThread; i++) {
							for (int j = 0; j < messagesPerBatch; j++) {
						        m[j] = source.dequeue();
								assertNotNull("Failed to dequeue", m[j]);
								if (j % 3 == 1) {
									nack(m[j].getAckId());
								} else if (j % 3 == 2) {
									ack(m[j].getAckId());
								} else { // out of order
									if (j % 2 == 1)
										yield();
								}
							}
							yield();
							for (int j = messagesPerBatch - 1; j >= 0; j--) {
								if (j % 3 == 1) {
									ack(m[j].getAckId());
								} else if (j % 3 == 2) {
									if (j % 2 == 0)
										yield();
								} else {
									ack(m[j].getAckId());
								}
							}
						}
					} catch (Throwable e) {
						e.printStackTrace();
						error = e;
					}
				}
			};
			ackers[acker].start();
		}
        for (Thread acker : ackers) {
            acker.join();
		}
        if (error != null)
            throw error;
		assertEquals(0, source.getInflightMetrics().getNumInFlight());
	}
	
	@Test
    public void testAckAfterDequeue() throws Exception {
        setUp(new AlwaysIncreasingClock());
        loadStore(4);
        StoredEntry<AckIdV3> m1 = source.dequeue();
        assertEquals(1, source.getInflightMetrics().getNumInFlight());
        ack(m1.getAckId());
        assertEquals(0, source.getInflightMetrics().getNumInFlight());

        StoredEntry<AckIdV3> m2 = source.dequeue();
        assertEquals(1, source.getInflightMetrics().getNumInFlight());
        ack(m2.getAckId());
        assertEquals(0, source.getInflightMetrics().getNumInFlight());

        StoredEntry<AckIdV3> m3 = source.dequeue();
        assertEquals(1, source.getInflightMetrics().getNumInFlight());
        ack(m3.getAckId());
        assertEquals(0, source.getInflightMetrics().getNumInFlight());

        StoredEntry<AckIdV3> m4 = source.dequeue();
        assertEquals(1, source.getInflightMetrics().getNumInFlight());
        ack(m4.getAckId());
        assertEquals(0, source.getInflightMetrics().getNumInFlight());

        assertTrue(m4.getAckId().compareTo(source.getAckLevel()) <= 0);
    }

	@Test
	public void testAckInOrder() throws Exception {
		setUp(new AlwaysIncreasingClock());
		loadStore(4);
		StoredEntry<AckIdV3> m1 = source.dequeue();
		StoredEntry<AckIdV3> m2 = source.dequeue();
		StoredEntry<AckIdV3> m3 = source.dequeue();
		StoredEntry<AckIdV3> m4 = source.dequeue();
		ack(m1.getAckId());
		ack(m2.getAckId());
		ack(m3.getAckId());
		ack(m4.getAckId());
		assertTrue(m4.getAckId().compareTo(source.getAckLevel()) <= 0);
	}

	

	@Test
	public void testDoubleAckAfterDequeue() throws Exception {
		setUp(new AlwaysIncreasingClock());
		loadStore(4);
		StoredEntry<AckIdV3> m1 = source.dequeue();
		assertEquals(1, source.getInflightMetrics().getNumInFlight());
		ack(m1.getAckId());
		assertEquals(0, source.getInflightMetrics().getNumInFlight());
		ack(m1.getAckId());
		assertEquals(0, source.getInflightMetrics().getNumInFlight());

		StoredEntry<AckIdV3> m2 = source.dequeue();
		assertEquals(1, source.getInflightMetrics().getNumInFlight());
		ack(m2.getAckId());
		assertEquals(0, source.getInflightMetrics().getNumInFlight());
		ack(m2.getAckId());
		assertEquals(0, source.getInflightMetrics().getNumInFlight());
		StoredEntry<AckIdV3> m3 = source.dequeue();
		assertEquals(1, source.getInflightMetrics().getNumInFlight());
		ack(m3.getAckId());
		assertEquals(0, source.getInflightMetrics().getNumInFlight());
		ack(m3.getAckId());
		assertEquals(0, source.getInflightMetrics().getNumInFlight());

		StoredEntry<AckIdV3> m4 = source.dequeue();
		assertEquals(1, source.getInflightMetrics().getNumInFlight());
		ack(m4.getAckId());
		assertEquals(0, source.getInflightMetrics().getNumInFlight());
		ack(m4.getAckId());
		assertEquals(0, source.getInflightMetrics().getNumInFlight());

		assertTrue(m4.getAckId().compareTo(source.getAckLevel()) <= 0);
	}

	@Test
	public void testNack() throws Exception {
		setUp(new AlwaysIncreasingClock());
		loadStore(4);
		StoredEntry<AckIdV3> m1 = source.dequeue();
		ack(m1.getAckId());
		StoredEntry<AckIdV3> m2 = source.dequeue();
		nack(m2.getAckId());
		StoredEntry<AckIdV3> m3 = source.dequeue();
		ack(m3.getAckId());
		StoredEntry<AckIdV3> m4 = source.dequeue();
		ack(m4.getAckId());
		assertEquals(m2.getAckId(), m3.getAckId());
		assertTrue(m4.getAckId().compareTo(source.getAckLevel()) <= 0);
	}

	@Test
	public void testDoubleNack() throws Exception {
		setUp(new AlwaysIncreasingClock());
		loadStore(4);
		StoredEntry<AckIdV3> m1 = source.dequeue();
		ack(m1.getAckId());
		StoredEntry<AckIdV3> m2 = source.dequeue();
		nack(m2.getAckId());
		nack(m2.getAckId());
		StoredEntry<AckIdV3> m3 = source.dequeue();
		ack(m3.getAckId());
		StoredEntry<AckIdV3> m4 = source.dequeue();
		ack(m4.getAckId());
		assertEquals(m2.getAckId(), m3.getAckId());
		assertNotSame(m3, m4);
		assertTrue(m4.getAckId().compareTo(source.getAckLevel()) <= 0);
	}

	@Test
	public void testNackAck() throws Exception {
		setUp(new AlwaysIncreasingClock());
		loadStore(4);
		StoredEntry<AckIdV3> m1 = source.dequeue();
		ack(m1.getAckId());
		StoredEntry<AckIdV3> m2 = source.dequeue();
		nack(m2.getAckId());
		ack(m2.getAckId());
		StoredEntry<AckIdV3> m3 = source.dequeue();
		ack(m3.getAckId());
		StoredEntry<AckIdV3> m4 = source.dequeue();
		ack(m4.getAckId());
		assertTrue(m4.getAckId().compareTo(source.getAckLevel()) <= 0);
	}

	@Test
	public void testAckNack() throws Exception {
		setUp(new AlwaysIncreasingClock());
		loadStore(4);
		StoredEntry<AckIdV3> m1 = source.dequeue();
		ack(m1.getAckId());
		StoredEntry<AckIdV3> m2 = source.dequeue();
		ack(m2.getAckId());
		nack(m2.getAckId());
		StoredEntry<AckIdV3> m3 = source.dequeue();
		ack(m3.getAckId());
		StoredEntry<AckIdV3> m4 = source.dequeue();
		ack(m4.getAckId());
		assertTrue(m4.getAckId().compareTo(source.getAckLevel()) <= 0);
	}

	@Test
	public void testAckInReverseOrder() throws Exception {
		setUp(new AlwaysIncreasingClock());
		loadStore(4);
		StoredEntry<AckIdV3> m1 = source.dequeue();
		StoredEntry<AckIdV3> m2 = source.dequeue();
		StoredEntry<AckIdV3> m3 = source.dequeue();
		StoredEntry<AckIdV3> m4 = source.dequeue();
		assertEquals(4, source.getInflightMetrics().getNumInFlight());
		ack(m4.getAckId());
		assertEquals(3, source.getInflightMetrics().getNumInFlight());
		ack(m3.getAckId());
		assertEquals(2, source.getInflightMetrics().getNumInFlight());
		ack(m2.getAckId());
		assertEquals(1, source.getInflightMetrics().getNumInFlight());
		ack(m1.getAckId());
		assertEquals(0, source.getInflightMetrics().getNumInFlight());
		assertTrue(m4.getAckId().compareTo(source.getAckLevel()) <= 0);
	}

	@Test
	public void testAckInRandomOrder() throws Exception {
		setUp(new AlwaysIncreasingClock());
		loadStore(4);
		StoredEntry<AckIdV3> m1 = source.dequeue();
		StoredEntry<AckIdV3> m2 = source.dequeue();
		StoredEntry<AckIdV3> m3 = source.dequeue();
		StoredEntry<AckIdV3> m4 = source.dequeue();
		assertEquals(4, source.getInflightMetrics().getNumInFlight());
		ack(m3.getAckId());
		assertEquals(3, source.getInflightMetrics().getNumInFlight());
		ack(m2.getAckId());
		assertEquals(2, source.getInflightMetrics().getNumInFlight());
		ack(m1.getAckId());
		assertEquals(1, source.getInflightMetrics().getNumInFlight());
		ack(m4.getAckId());
		assertEquals(0, source.getInflightMetrics().getNumInFlight());
		assertTrue(m4.getAckId().compareTo(source.getAckLevel()) <= 0);
	}

	@Test
	public void testAckInRandomOrderWhileGetting() throws Exception {
		setUp(new AlwaysIncreasingClock());
		loadStore(4);
		StoredEntry<AckIdV3> m1 = source.dequeue();
		StoredEntry<AckIdV3> m2 = source.dequeue();
		assertEquals(2, source.getInflightMetrics().getNumInFlight());
		ack(m2.getAckId());
		assertEquals(1, source.getInflightMetrics().getNumInFlight());
		StoredEntry<AckIdV3> m3 = source.dequeue();
		StoredEntry<AckIdV3> m4 = source.dequeue();
		assertEquals(3, source.getInflightMetrics().getNumInFlight());
		ack(m4.getAckId());
		assertEquals(2, source.getInflightMetrics().getNumInFlight());
		ack(m1.getAckId());
		assertEquals(1, source.getInflightMetrics().getNumInFlight());
		ack(m3.getAckId());
		assertEquals(0, source.getInflightMetrics().getNumInFlight());
		assertTrue(m4.getAckId().compareTo(source.getAckLevel()) <= 0);
	}

	@Test
	public void testFullFailureSingleInFlight() throws Exception {
	    setUp(new SettableClock());
		final int numMessages = 1000;
		loadStore(numMessages);
		Random r = new Random();
		double failureProbability = 0.9;
		// int failureCount = 0;
		int dequeueCount = 0;
		int ackedCount = 0;
		int nackCount = 0;
		StoredEntry<AckIdV3> m = source.dequeue();
		while (m != null) {
			dequeueCount++;
			
			float next = r.nextFloat();
			if (next < failureProbability) {
				nack(m.getAckId());
				nackCount++;
			} else {
				ack(m.getAckId());
				ackedCount++;
			}
			
			m = source.dequeue();
		}

		assertEquals(0, source.getInflightMetrics().getNumInFlight());
		assertEquals(ackedCount, numMessages);
		assertEquals(dequeueCount, nackCount + ackedCount);
	}

	@Test
	public void testPartialFailureSingleInFlight() throws Exception {
		setUp(new SettableClock());
        final int numMessages = 10000;
		loadStore(numMessages);
		Random r = new Random();
		double failureProbability = 0.5;
		// int failureCount = 0;
		int dequeueCount = 0;
		int ackedCount = 0;
		int nackCount = 0;
		StoredEntry<AckIdV3> m = source.dequeue();
		while (m != null) {
			dequeueCount++;
			
			float next = r.nextFloat();
			if (next < failureProbability) {
				nack(m.getAckId());
				nackCount++;
			} else {
				ack(m.getAckId());
				ackedCount++;
			}
			
			m = source.dequeue();
		}

		assertEquals(0, source.getInflightMetrics().getNumInFlight());
		assertEquals(ackedCount, numMessages);
		assertEquals(dequeueCount, nackCount + ackedCount );
	}

	@Test
	public void testFullFailureMultipleInFlight() throws Exception {
		setUp(new AlwaysIncreasingClock());
		loadStore(1000);
		int failureCount = 0;
		int nextCount = 0;

		AckIdV3 lastId = ackGen_.getAckId(0);
		AckIdV3 lastAck = ackGen_.getAckId(0);
		Vector<AckIdV3> nacks = new Vector<AckIdV3>();
		for (int i = 0; i < 1000; i++) {
			StoredEntry<AckIdV3> m = source.dequeue();
			lastId = m.getAckId();
			nextCount++;
			if (i >= 600) {
				ack(m.getAckId());
				lastAck = m.getAckId();
			} else {
				nacks.add(m.getAckId());
				// nack(m.getAckId());
				failureCount++;
			}
		}
		for (int i = 0; i < failureCount; i++) {
			nack(nacks.get(i));
		}
		assertEquals(0, source.getInflightMetrics().getNumInFlight());
		while (failureCount > 0) {
			assertTrue(null != source.dequeue());
			failureCount--;
		}
		assertTrue(null == source.dequeue());
		// assertEquals(failureCount, nextCount -
		// AckIdGenerator.getIndexFromData(lastId));
		assertEquals(lastId, lastAck);
	}

	@Test
	public void testPartialFailureMultipleInFlight() throws Exception {
	    setUp(new SettableClock());
		final int numMessages = 1000;
		loadStore(numMessages);
		Random r = new Random();
		float failureProbability = 0.5f;
		int ackCount = 0;
		int nackCount = 0;
		int dequeueCount = 0;
		long processed = 0;
		
        Set<AckIdV3> unacked = new LinkedHashSet<AckIdV3>();
        long startTime = System.currentTimeMillis();
        
		StoredEntry<AckIdV3> m = source.dequeue();
        while (m != null && System.currentTimeMillis() - startTime < 20000) {
			dequeueCount++;
			
			assertFalse(unacked.contains(m.getAckId()));
			
			if (processed >= 600) {
				failureProbability = 0.0f;
				assertTrue(ack(m.getAckId()));
				ackCount++;
			} else if (r.nextFloat() < failureProbability) {
				assertTrue(nack(m.getAckId()));
				nackCount++;
			} else {
			    assertTrue(extendTimeout(m.getAckId(), 10));
			    
			    InflightEntryInfo<AckIdV3, BasicInflightInfo> mInfo =
			        source.getInFlightInfo(m.getAckId());
			    assertEquals(10, mInfo.getDelayUntilNextRedrive());
			    
				unacked.add(m.getAckId());
			}
			processed++;
			
			m = source.dequeue();
        }

		for (AckIdV3 toAck : unacked) {
			ack(toAck);
			ackCount++;
		}

		assertEquals(0, source.getInflightMetrics().getNumInFlight());
		assertEquals(dequeueCount, nackCount + ackCount);
		assertEquals(numMessages, ackCount);
	}

    @Test
    public void testTimeout() throws Exception {
        SeqStoreReaderConfig<BasicInflightInfo> config =
            new SeqStoreReaderConfig<BasicInflightInfo>(new BasicInflightInfoFactory(1000));
        
        SettableClock clock = new SettableClock();
        setUp(clock, config);
        loadStore(2);
        
        InflightEntry<AckIdV3, BasicInflightInfo> entry1 = source.dequeue();
        assertNotNull(entry1);
        assertNotNull(entry1.getAckId());
        assertEquals(1000, entry1.getDelayUntilNextRedrive());
        assertEquals(1, entry1.getInflightInfo().getDeliveryCount());
        
        clock.increaseTime(5);
        
        InflightEntry<AckIdV3, BasicInflightInfo> entry2 = source.dequeue();
        assertNotNull(entry2);
        assertNotNull(entry2.getAckId());
        assertEquals(1000, entry2.getDelayUntilNextRedrive());
        assertEquals(1, entry2.getInflightInfo().getDeliveryCount());
        assertEquals(null, source.dequeue() );
        
        clock.increaseTime(994);
        assertEquals(null, source.dequeue() );
        
        clock.increaseTime(1);
        InflightEntry<AckIdV3, BasicInflightInfo> entry1again = source.dequeue();
        assertNotNull(entry1again);
        assertEquals(entry1.getAckId(), entry1again.getAckId());
        assertEquals(1000, entry1again.getDelayUntilNextRedrive());
        assertEquals(2, entry1again.getInflightInfo().getDeliveryCount());
        
        assertEquals(null, source.dequeue() );
        clock.increaseTime(4);
        assertEquals(null, source.dequeue() );
        clock.increaseTime(1);
        
        InflightEntry<AckIdV3, BasicInflightInfo> entry2again = source.dequeue();
        assertNotNull(entry2again);
        assertEquals(entry2.getAckId(), entry2again.getAckId());
        assertEquals(1000, entry2again.getDelayUntilNextRedrive());
        assertEquals(2, entry2again.getInflightInfo().getDeliveryCount());
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
