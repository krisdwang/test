package com.amazon.messaging.seqstore.v3.internal;



import static org.junit.Assert.*;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.concurrent.DeterministicScheduler;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazon.messaging.impls.SettableClock;
import com.amazon.messaging.seqstore.v3.InflightEntry;
import com.amazon.messaging.seqstore.v3.MessageListener;
import com.amazon.messaging.seqstore.v3.SeqStore;
import com.amazon.messaging.seqstore.v3.TestEntry;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreManager;
import com.amazon.messaging.seqstore.v3.mapPersistence.MapStoreManager;
import com.amazon.messaging.testing.TestCase;
import com.amazon.messaging.util.BasicConfigProvider;
import com.amazon.messaging.util.BasicInflightInfo;



/**
 * Tests the message listener interface.
 */
public class SeqStoreReaderMessageListenerTest extends TestCase {

    SeqStore<AckIdV3, BasicInflightInfo> store = null;
    TopDisposableReader<BasicInflightInfo> source = null;

    final DeterministicScheduler executor = new DeterministicScheduler ();

    final SettableClock clock = new SettableClock (1);
    final Mockery context = new Mockery ();

    SeqStoreManager<AckIdV3, BasicInflightInfo> manager = null;

    @Before
    public void setUp () throws Exception {
        manager = new MapStoreManager<BasicInflightInfo> (BasicConfigProvider.newBasicInflightConfigProvider (), clock);

        store = manager.createStore (new StoreIdImpl ("store"));

        store.createReader ("reader");
        source = (TopDisposableReader<BasicInflightInfo>) store.getReader ("reader");
        
        MessageListenerManager.setCatchAssertionErrors(false);
    }

    @After
    public void tearDown () throws Exception {
        source.setMessageListener (null, null);
        manager.close ();
    }

    @Test
    @SuppressWarnings ("unchecked")
    public void testMessageListener () throws InterruptedException, SeqStoreException {
        final MessageListener<AckIdV3, BasicInflightInfo> listener = context.mock (MessageListener.class);
        source.setMessageListener (executor, listener);

        // First there shouldn't be any messages delivered because it's not their time
        //
        context.checking (new Expectations () {
            {}
        });

        store.enqueue (new TestEntry ("Message0", 100), -1, null);
        store.enqueue (new TestEntry ("Message1", 200), -1, null);

        context.assertIsSatisfied ();

        // Once time ticks 100, the first two enqueued messages above should be delivered
        //
        context.checking (new Expectations () {
            {
                oneOf (listener).onMessage (with (aNonNull (InflightEntry.class)));
            }
        });

        clock.setCurrentTime (100);
        executor.tick (100, TimeUnit.MILLISECONDS);

        context.assertIsSatisfied ();

        // The second of the two messages should be delivered at time 200
        //
        context.checking (new Expectations () {
            {
                oneOf (listener).onMessage (with (aNonNull (InflightEntry.class)));
            }
        });
        clock.setCurrentTime (200);
        executor.tick (100, TimeUnit.MILLISECONDS);

        context.assertIsSatisfied ();
        
        context.checking (new Expectations () {
            {
                oneOf (listener).shutdown();
            }
        });
        
        source.setMessageListener (null, null);
        executor.runUntilIdle ();
        
        context.assertIsSatisfied ();
    }
    
    @Test
    @SuppressWarnings ("unchecked")
    public void testMessageListenerExceptionHandling () throws InterruptedException, SeqStoreException {
        final MessageListener<AckIdV3, BasicInflightInfo> listener = context.mock (MessageListener.class);
        source.setMessageListener (executor, listener);

        // First there shouldn't be any messages delivered because it's not their time
        //
        context.checking (new Expectations () {
            {}
        });

        store.enqueue (new TestEntry ("Message0", 100), -1, null);
        store.enqueue (new TestEntry ("Message1", 200), -1, null);

        context.assertIsSatisfied ();

        // Once time ticks 100, the first two enqueued messages above should be delivered
        //
        context.checking (new Expectations () {
            {
                oneOf (listener).onMessage (with (aNonNull (InflightEntry.class)));
                will( throwException(new RuntimeException("testException") ) );
            }
        });

        clock.setCurrentTime (100);
        executor.tick (100, TimeUnit.MILLISECONDS);

        context.assertIsSatisfied ();

        // The second of the two messages should be delivered at time 200 even if the first
        // one threw an exception
        //
        context.checking (new Expectations () {
            {
                oneOf (listener).onMessage (with (aNonNull (InflightEntry.class)));
            }
        });
        clock.setCurrentTime (200);
        executor.tick (100, TimeUnit.MILLISECONDS);

        context.assertIsSatisfied ();
        
        context.checking (new Expectations () {
            {
                oneOf (listener).shutdown();
            }
        });
        
        source.setMessageListener (null, null);
        executor.runUntilIdle ();
        
        context.assertIsSatisfied ();
    }
    
    private class FakeError extends Error {
        private static final long serialVersionUID = 1L;

        public FakeError() {
            super("Fake Error for testing");
        }
    }
    
    @Test
    @SuppressWarnings ("unchecked")
    public void testMessageListenerErrorHandling () throws InterruptedException, SeqStoreException {
        final MessageListener<AckIdV3, BasicInflightInfo> listener = context.mock (MessageListener.class);
        source.setMessageListener (executor, listener);

        // First there shouldn't be any messages delivered because it's not their time
        //
        context.checking (new Expectations () {
            {}
        });

        store.enqueue (new TestEntry ("Message0", 100), -1, null);
        store.enqueue (new TestEntry ("Message1", 200), -1, null);

        context.assertIsSatisfied ();

        // Once time ticks 100, the first two enqueued messages above should be delivered
        //
        context.checking (new Expectations () {
            {
                oneOf (listener).onMessage (with (aNonNull (InflightEntry.class)));
                will( throwException(new FakeError() ) );
            }
        });

        clock.setCurrentTime (100);
        try {
            executor.tick (100, TimeUnit.MILLISECONDS);
            fail( "Expected FakeError to be thrown");
        } catch( FakeError e ) {
            // Success
        }

        context.assertIsSatisfied ();

        // The second of the two messages should be delivered at time 200 even if the first
        // one threw an exception
        //
        context.checking (new Expectations () {
            {
                oneOf (listener).onMessage (with (aNonNull (InflightEntry.class)));
            }
        });
        clock.setCurrentTime (200);
        executor.tick (100, TimeUnit.MILLISECONDS);

        context.assertIsSatisfied ();
        
        context.checking (new Expectations () {
            {
                oneOf (listener).shutdown();
            }
        });
        
        source.setMessageListener (null, null);
        executor.runUntilIdle ();
        
        context.assertIsSatisfied ();
    }

    @Test
    @SuppressWarnings ("unchecked")
    public void testMessageListenerAfterMessages () throws InterruptedException, SeqStoreException {
        final MessageListener<AckIdV3, BasicInflightInfo> listener = context.mock (MessageListener.class);

        store.enqueue (new TestEntry ("Message0", 100), -1, null);
        store.enqueue (new TestEntry ("Message1", 200), -1, null);

        clock.setCurrentTime (300);
        executor.tick (300, TimeUnit.MILLISECONDS);

        source.setMessageListener (executor, listener);

        // Both messages should be delivered as soon as the listener is added
        //
        context.checking (new Expectations () {
            {
                exactly(2).of (listener).onMessage (with (aNonNull (InflightEntry.class)));
            }
        });

        executor.tick (0, TimeUnit.MILLISECONDS);

        context.assertIsSatisfied ();
        
        context.checking (new Expectations () {
            {
                oneOf (listener).shutdown();
            }
        });
        
        source.setMessageListener (null, null);
        executor.runUntilIdle ();
        
        context.assertIsSatisfied ();
    }

    @Test
    public void ensureRemoveMessageReaderIsNonBlocking () throws Exception {
        store.enqueue (new TestEntry ("Message0", 0), -1, null);

        final AtomicReference<Future<Void>> refRemoveFuture = new AtomicReference<Future<Void>> ();
        final MessageListener<AckIdV3, BasicInflightInfo> listener = new MessageListener<AckIdV3, BasicInflightInfo>() {
            @Override
            public void onMessage (InflightEntry<AckIdV3, BasicInflightInfo> entry) {
                refRemoveFuture.set (source.removeMessageListener (this));
            }

            @Override
            public void onException (SeqStoreException e) {
                fail (e.toString ());
            }

            @Override
            public void shutdown () {

            }
        };

        source.setMessageListener (executor, listener);
        executor.runUntilIdle ();
        
        assertNotNull (refRemoveFuture.get ());
        assertNull (refRemoveFuture.get ().get (0, TimeUnit.MILLISECONDS));
    }
}
