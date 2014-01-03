package com.amazon.messaging.seqstore.v3.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import lombok.RequiredArgsConstructor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;
import org.hamcrest.Description;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.Sequence;
import org.jmock.api.Action;
import org.jmock.api.Invocation;
import org.jmock.integration.junit4.JMock;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.amazon.messaging.impls.SettableClock;
import com.amazon.messaging.impls.SystemClock;
import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.v3.InflightEntry;
import com.amazon.messaging.seqstore.v3.MessageListener;
import com.amazon.messaging.seqstore.v3.TestEntry;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreInternalException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreUnrecoverableDatabaseException;
import com.amazon.messaging.seqstore.v3.internal.MessageListenerManager.MessageSource;
import com.amazon.messaging.testing.DeterministicScheduledExecutorService;
import com.amazon.messaging.testing.JVMStatusLogger;
import com.amazon.messaging.testing.TestCase;
import com.amazon.messaging.testing.TestExecutorService;
import com.amazon.messaging.testing.TestScheduledExecutorService;
import com.amazon.messaging.util.BasicInflightInfo;


@RunWith(JMock.class)
public class MessageListenerManagerTest extends TestCase {
    private static final Log log = LogFactory.getLog(MessageListenerManagerTest.class);
    
    private final Mockery context = new Mockery() {{
        setImposteriser(ClassImposteriser.INSTANCE);
    }};
    
    private SettableClock clock;
    private DeterministicScheduledExecutorService executor;
    private MessageSource<AckIdV3, BasicInflightInfo> mockMessageSource;
    private MessageListener<AckIdV3, BasicInflightInfo> mockMessageListener;
    
    @SuppressWarnings("unchecked")
    @Before
    public void setup() {
        clock = new SettableClock (1);
        executor = new DeterministicScheduledExecutorService ();
        mockMessageSource = context.mock(MessageSource.class);
        mockMessageListener = context.mock(MessageListener.class);
        clock.setCurrentTime(1);
        MessageListenerManager.setCatchAssertionErrors(false);
    }
    
    @Test
    public void testDelivery() throws SeqStoreException {
        context.checking (new Expectations () {
            {
                oneOf(mockMessageSource).getTimeOfNextMessage(); will( returnValue( 100L ) );
            }
        });
        
        MessageListenerManager<AckIdV3, BasicInflightInfo> messageListenerManager 
            = new MessageListenerManager<AckIdV3, BasicInflightInfo>(
                clock, mockMessageSource, mockMessageListener, executor);
        
        assertEquals( 1, executor.getNumScheduled() );
        // The message isn't ready yet
        assertTrue( executor.isIdle() );
        
        final Sequence sequence = context.sequence("Sequence");
        
        context.checking (new Expectations () {
            {
                for( int i = 0; i < MessageListenerManager.MaxMessagesPerCall; i++ ) {
                    @SuppressWarnings("unchecked")
                    InflightEntry<AckIdV3, BasicInflightInfo> dequeuedEntry = 
                            context.mock( InflightEntry.class, "Entry" + i );
                    
                    oneOf(mockMessageSource).dequeue(); will( returnValue( dequeuedEntry ) );
                    inSequence(sequence);
                    
                    oneOf(mockMessageListener).onMessage( dequeuedEntry );
                    inSequence(sequence);
                }
                
                // After MaxMessagesPerCall messages prompt for the next time
                oneOf(mockMessageSource).getTimeOfNextMessage(); will( returnValue( 100L ) );
                inSequence(sequence);
                
                @SuppressWarnings("unchecked")
                InflightEntry<AckIdV3, BasicInflightInfo> finalDequeuedEntry = 
                        context.mock( InflightEntry.class, "Entry10" );
                oneOf(mockMessageSource).dequeue(); will( returnValue( finalDequeuedEntry ) );
                inSequence(sequence);
                
                oneOf(mockMessageListener).onMessage( finalDequeuedEntry );
                inSequence(sequence);
                
                oneOf(mockMessageSource).dequeue(); will( returnValue(null) );
                inSequence(sequence);
                
                oneOf(mockMessageSource).getTimeOfNextMessage(); will( returnValue( 200L ) );
                inSequence(sequence);
            }
        });
        
        clock.increaseTime(99);
        
        // This should deliver all the messages
        executor.tick(99, TimeUnit.MILLISECONDS);
        
        // Check all the calls happened
        context.assertIsSatisfied();
        
        // The future scheduled task should still be there
        assertEquals( 1, executor.getNumScheduled() );
        
        context.checking (new Expectations () {
            {
                @SuppressWarnings("unchecked")
                InflightEntry<AckIdV3, BasicInflightInfo> dequeuedEntry = 
                        context.mock( InflightEntry.class, "Entry11" );
                oneOf(mockMessageSource).dequeue(); will( returnValue( dequeuedEntry ) );
                inSequence(sequence);
                
                oneOf(mockMessageListener).onMessage( dequeuedEntry );
                inSequence(sequence);
                
                oneOf(mockMessageSource).dequeue(); will( returnValue(null ) );
                inSequence(sequence);
                
                oneOf(mockMessageSource).getTimeOfNextMessage(); will( returnValue( Long.MAX_VALUE ) );
                inSequence(sequence);
            }
        });
        
        clock.increaseTime(100);
        executor.tick(100, TimeUnit.MILLISECONDS);
        
        // Check all the calls happened
        context.assertIsSatisfied();
        
        // Nothing should be scheduled
        assertEquals( 0, executor.getNumScheduled() );
        
        messageListenerManager.newMessageAvailable(220);
        
        assertEquals( 1, executor.getNumScheduled() );
        // The task shouldn't be ready yet
        assertTrue( executor.isIdle() );
        
        context.checking (new Expectations () {
            {
                @SuppressWarnings("unchecked")
                InflightEntry<AckIdV3, BasicInflightInfo> dequeuedEntry = 
                        context.mock( InflightEntry.class, "Entry12" );
                oneOf(mockMessageSource).dequeue(); will( returnValue( dequeuedEntry ) );
                inSequence(sequence);
                
                oneOf(mockMessageListener).onMessage( dequeuedEntry );
                inSequence(sequence);
                
                oneOf(mockMessageSource).dequeue(); will( returnValue(null ) );
                inSequence(sequence);
                
                oneOf(mockMessageSource).getTimeOfNextMessage(); will( returnValue( Long.MAX_VALUE ) );
                inSequence(sequence);
            }
        });
        
        clock.increaseTime(20);
        executor.tick(20, TimeUnit.MILLISECONDS);
        
        // Check all the calls happened
        context.assertIsSatisfied();
        
        // Nothing should be scheduled
        assertEquals( 0, executor.getNumScheduled() );
    }
    
    @Test
    public void testDeliveryStartIdle() throws SeqStoreException {
        context.checking (new Expectations () {
            {
                oneOf(mockMessageSource).getTimeOfNextMessage(); will( returnValue( Long.MAX_VALUE ) );
            }
        });
        
        MessageListenerManager<AckIdV3, BasicInflightInfo> messageListenerManager 
            = new MessageListenerManager<AckIdV3, BasicInflightInfo>(
                clock, mockMessageSource, mockMessageListener, executor);
        
        assertEquals( 0, executor.getNumScheduled() );
        
        final Sequence sequence = context.sequence("Sequence");
        
        context.checking (new Expectations () {
            {
                @SuppressWarnings("unchecked")
                InflightEntry<AckIdV3, BasicInflightInfo> finalDequeuedEntry = 
                        context.mock( InflightEntry.class, "Entry0" );
                oneOf(mockMessageSource).dequeue(); will( returnValue( finalDequeuedEntry ) );
                inSequence(sequence);
                
                oneOf(mockMessageListener).onMessage( finalDequeuedEntry );
                inSequence(sequence);
                
                oneOf(mockMessageSource).dequeue(); will( returnValue(null) );
                inSequence(sequence);
                
                oneOf(mockMessageSource).getTimeOfNextMessage(); will( returnValue( Long.MAX_VALUE ) );
                inSequence(sequence);
            }
        });
        
        messageListenerManager.newMessageAvailable(100);
        assertEquals( 1, executor.getNumScheduled() );
        assertTrue( executor.isIdle() );
        
        clock.increaseTime(99);
        
        // This should deliver all the messages
        executor.tick(99, TimeUnit.MILLISECONDS);
        
        // Check all the calls happened
        context.assertIsSatisfied();
        
        assertEquals( 0, executor.getNumScheduled() );
    }
    
    @Test
    public void testDequeueThrowsRuntimeException() throws SeqStoreException {
        testDequeueThrows(new RuntimeException("test"));
    }
    
    @Test
    public void testDequeueThrowsSeqStoreException() throws SeqStoreException {
        testDequeueThrows(new SeqStoreInternalException("test"));
    }
    
    private static class TestError extends Error {
        private static final long serialVersionUID = 1L;
    }
    
    @Test
    public void testDequeueThrowsError() throws SeqStoreException {
        testDequeueThrows( new TestError() );
    }

    private void testDequeueThrows(final Throwable throwableToThrow) throws SeqStoreException {
        context.checking (new Expectations () {
            {
                oneOf(mockMessageSource).getTimeOfNextMessage(); will( returnValue( 100L ) );
            }
        });
        
        @SuppressWarnings("unused")
        MessageListenerManager<AckIdV3, BasicInflightInfo> messageListenerManager 
            = new MessageListenerManager<AckIdV3, BasicInflightInfo>(
                clock, mockMessageSource, mockMessageListener, executor);
        
        assertEquals( 1, executor.getNumScheduled() );
        
        final Sequence sequence = context.sequence("Sequence");
        
        context.checking (new Expectations () {
            {
                oneOf(mockMessageSource).dequeue(); will( throwException(throwableToThrow) );
                inSequence(sequence);
                
                if( throwableToThrow instanceof SeqStoreException ) {
                    oneOf(mockMessageListener).onException((SeqStoreException) throwableToThrow );
                    inSequence(sequence);
                } else if( throwableToThrow instanceof Exception ) {
                    oneOf(mockMessageListener).onException(with(any(SeqStoreException.class)));
                }
                
                oneOf(mockMessageSource).getTimeOfNextMessage(); will(returnValue(110L));
            }
        });
        
        clock.increaseTime(99);
        
        // This should deliver all the messages
        try {
            executor.tick(99, TimeUnit.MILLISECONDS);
        } catch (TestError e ) {
            // A normal executor would have caught the Error
            assertSame( throwableToThrow, e );
        }
        
        context.assertIsSatisfied();
        
        // The task should have been rescheduled
        assertEquals( 1, executor.getNumScheduled() );
        // But not immediately
        assertTrue( executor.isIdle() );
        
        context.checking (new Expectations () {
            {
                @SuppressWarnings("unchecked")
                InflightEntry<AckIdV3, BasicInflightInfo> finalDequeuedEntry = 
                        context.mock( InflightEntry.class, "Entry0" );
                oneOf(mockMessageSource).dequeue(); will( returnValue( finalDequeuedEntry ) );
                inSequence(sequence);
                
                oneOf(mockMessageListener).onMessage( finalDequeuedEntry );
                inSequence(sequence);
                
                oneOf(mockMessageSource).dequeue(); will( returnValue(null) );
                inSequence(sequence);
                
                oneOf(mockMessageSource).getTimeOfNextMessage(); will( returnValue( Long.MAX_VALUE ) );
                inSequence(sequence);
            }
        });
        
        if( throwableToThrow instanceof Error ) {
            clock.increaseTime( MessageListenerManager.RetryOnExceptionDelayMillis );
            executor.tick(MessageListenerManager.RetryOnExceptionDelayMillis, TimeUnit.MILLISECONDS);
        } else {
            clock.increaseTime( 10 );
            executor.tick(10, TimeUnit.MILLISECONDS);
        }
        
        // Check all the calls happened
        context.assertIsSatisfied();
        
        assertEquals( 0, executor.getNumScheduled() );
    }
    
    @Test
    public void testGetTimeOfNextMessageThrowsRuntimeException() throws SeqStoreException {
        testGetTimeOfNextMessageThrows(new RuntimeException("test"));
    }
    
    @Test
    public void testGetTimeOfNextMessageThrowsSeqStoreException() throws SeqStoreException {
        testGetTimeOfNextMessageThrows(new SeqStoreDatabaseException("test"));
    }
    
    @Test
    public void testGetTimeOfNextMessageThrowsSeqStoreUnrecoverableException() throws SeqStoreException {
        testGetTimeOfNextMessageThrows(new SeqStoreUnrecoverableDatabaseException("test", "message"));
    }
    
    @Test
    public void testGetTimeOfNextMessageThrowsError() throws SeqStoreException {
        testGetTimeOfNextMessageThrows(new TestError());
    }

    private void testGetTimeOfNextMessageThrows(final Throwable throwableToThrow) 
            throws SeqStoreException 
    {
        context.checking (new Expectations () {
            {
                oneOf(mockMessageSource).getTimeOfNextMessage(); will( returnValue( 100L ) );
            }
        });
        
        MessageListenerManager<AckIdV3, BasicInflightInfo> messageListenerManager 
            = new MessageListenerManager<AckIdV3, BasicInflightInfo>(
                clock, mockMessageSource, mockMessageListener, executor);
        
        assertEquals( 1, executor.getNumScheduled() );
        
        final Sequence sequence = context.sequence("Sequence");
        
        context.checking (new Expectations () {
            {
                oneOf(mockMessageSource).dequeue(); will( returnValue(null) );
                inSequence(sequence);
                
                oneOf(mockMessageSource).getTimeOfNextMessage(); will( throwException( throwableToThrow ) );
                inSequence(sequence);
                
                if( throwableToThrow instanceof SeqStoreException ) {
                    oneOf(mockMessageListener).onException((SeqStoreException) throwableToThrow );
                    inSequence(sequence);
                }
            }
        });
        
        clock.increaseTime(99);
        
        // This should deliver all the messages
        try {
            executor.tick(99, TimeUnit.MILLISECONDS);
        } catch (TestError e ) {
            // A normal executor would have caught the Error
            assertSame( throwableToThrow, e );
        }
        
        context.assertIsSatisfied();
        
        if( throwableToThrow instanceof SeqStoreUnrecoverableDatabaseException ) {
            // Nothing should be scheduled until a new enqueue succeeds (which shouldn't happen if
            //  a SeqStoreUnrecoverableDatabaseException happens but shouldn't break either)
            assertEquals( 0, executor.getNumScheduled() );
            
            messageListenerManager.newMessageAvailable( 
                    clock.getCurrentTime() + MessageListenerManager.RetryOnExceptionDelayMillis );
        }
        
        // The task should have been rescheduled
        assertEquals( 1, executor.getNumScheduled() );
        // But not immediately
        assertTrue( executor.isIdle() );
        
        context.checking (new Expectations () {
            {
                @SuppressWarnings("unchecked")
                InflightEntry<AckIdV3, BasicInflightInfo> finalDequeuedEntry = 
                        context.mock( InflightEntry.class, "Entry0" );
                oneOf(mockMessageSource).dequeue(); will( returnValue( finalDequeuedEntry ) );
                inSequence(sequence);
                
                oneOf(mockMessageListener).onMessage( finalDequeuedEntry );
                inSequence(sequence);
                
                oneOf(mockMessageSource).dequeue(); will( returnValue(null) );
                inSequence(sequence);
                
                oneOf(mockMessageSource).getTimeOfNextMessage(); will( returnValue( Long.MAX_VALUE ) );
                inSequence(sequence);
            }
        });
        
        clock.increaseTime( MessageListenerManager.RetryOnExceptionDelayMillis );
        executor.tick(MessageListenerManager.RetryOnExceptionDelayMillis, TimeUnit.MILLISECONDS);
        
        // Check all the calls happened
        context.assertIsSatisfied();
        
        assertEquals( 0, executor.getNumScheduled() );
    }
    
    @Test
    public void testShutdown() throws SeqStoreException {
        context.checking (new Expectations () {
            {
                oneOf(mockMessageSource).getTimeOfNextMessage(); will( returnValue( 100L ) );
            }
        });
        
        MessageListenerManager<AckIdV3, BasicInflightInfo> messageListenerManager 
            = new MessageListenerManager<AckIdV3, BasicInflightInfo>(
                clock, mockMessageSource, mockMessageListener, executor);
        
        assertEquals( 1, executor.getNumScheduled() );
        // The message isn't ready yet
        assertTrue( executor.isIdle() );
        
        context.checking (new Expectations () {
            {
                oneOf(mockMessageListener).shutdown();
            }
        });
        
        messageListenerManager.shutdown();
        
        // Just the scheduled shutdown task
        assertEquals( 1, executor.getNumScheduled() );
        assertFalse( executor.isIdle() );
        
        executor.runUntilIdle();
        
        assertEquals( 0, executor.getNumScheduled() );
    }
    
    @Test
    public void testEarlierMessageAvailable() throws SeqStoreException {
        context.checking (new Expectations () {
            {
                oneOf(mockMessageSource).getTimeOfNextMessage(); will( returnValue( 100L ) );
            }
        });
        
        MessageListenerManager<AckIdV3, BasicInflightInfo> messageListenerManager 
            = new MessageListenerManager<AckIdV3, BasicInflightInfo>(
                clock, mockMessageSource, mockMessageListener, executor);
        
        assertEquals( 1, executor.getNumScheduled() );
        // The message isn't ready yet
        assertTrue( executor.isIdle() );
        
        // New message is available now
        messageListenerManager.newMessageAvailable(1);
        
        assertEquals( 1, executor.getNumScheduled() );
        // The message isn't ready yet
        assertFalse( executor.isIdle() );
        
        final Sequence sequence = context.sequence("Sequence");
        
        context.checking (new Expectations () {
            {
                @SuppressWarnings("unchecked")
                InflightEntry<AckIdV3, BasicInflightInfo> dequeuedEntry = 
                        context.mock( InflightEntry.class, "Entry0" );
                oneOf(mockMessageSource).dequeue(); will( returnValue( dequeuedEntry ) );
                inSequence(sequence);
                
                oneOf(mockMessageListener).onMessage( dequeuedEntry );
                inSequence(sequence);
                
                oneOf(mockMessageSource).dequeue(); will( returnValue(null) );
                inSequence(sequence);
                
                oneOf(mockMessageSource).getTimeOfNextMessage(); will( returnValue( 100L ) );
                inSequence(sequence);
            }
        });
        
        executor.runUntilIdle();
        
        // Check all the calls happened
        context.assertIsSatisfied();
        
        // The future scheduled task should still be there
        assertEquals( 1, executor.getNumScheduled() );
        
        context.checking (new Expectations () {
            {
                @SuppressWarnings("unchecked")
                InflightEntry<AckIdV3, BasicInflightInfo> dequeuedEntry = 
                        context.mock( InflightEntry.class, "Entry1" );
                oneOf(mockMessageSource).dequeue(); will( returnValue( dequeuedEntry ) );
                inSequence(sequence);
                
                oneOf(mockMessageListener).onMessage( dequeuedEntry );
                inSequence(sequence);
                
                oneOf(mockMessageSource).dequeue(); will( returnValue(null ) );
                inSequence(sequence);
                
                oneOf(mockMessageSource).getTimeOfNextMessage(); will( returnValue( Long.MAX_VALUE ) );
                inSequence(sequence);
            }
        });
        
        clock.increaseTime(99);
        executor.tick(99, TimeUnit.MILLISECONDS);
        
        // Check all the calls happened
        context.assertIsSatisfied();
        
        // Nothing should be scheduled
        assertEquals( 0, executor.getNumScheduled() );
    }
    
    @RequiredArgsConstructor
    private class NewMessageAvailableDuringGetTimeOfNextMessageAction implements Action {
        private final MessageListenerManager<?,?> manager;
        private final long newMessageTime;
        private final long returnValue;

        @Override
        public void describeTo(Description description) {
            description.appendText(
                    "call newMessageAvailable(" + newMessageTime + ") and return " + returnValue );
        }

        @Override
        public Object invoke(Invocation invocation) throws Throwable {
            manager.newMessageAvailable(newMessageTime);
            return returnValue;
        }
        
        
    }
    
    @Test
    public void testEarlierMessageAvailableDuringGetTimeOfNextMessage() throws SeqStoreException {
        context.checking (new Expectations () {
            {
                oneOf(mockMessageSource).getTimeOfNextMessage(); will( returnValue( 100L ) );
            }
        });
        
        final MessageListenerManager<AckIdV3, BasicInflightInfo> messageListenerManager 
            = new MessageListenerManager<AckIdV3, BasicInflightInfo>(
                clock, mockMessageSource, mockMessageListener, executor);
        
        assertEquals( 1, executor.getNumScheduled() );
        // The message isn't ready yet
        assertTrue( executor.isIdle() );
        
        final Sequence sequence = context.sequence("Sequence");
        
        context.checking (new Expectations () {
            {
                @SuppressWarnings("unchecked")
                InflightEntry<AckIdV3, BasicInflightInfo> dequeuedEntry = 
                        context.mock( InflightEntry.class, "Entry0" );
                oneOf(mockMessageSource).dequeue(); will( returnValue( dequeuedEntry ) );
                inSequence(sequence);
                
                oneOf(mockMessageListener).onMessage( dequeuedEntry );
                inSequence(sequence);
                
                oneOf(mockMessageSource).dequeue(); will( returnValue(null) );
                inSequence(sequence);
                
                oneOf(mockMessageSource).getTimeOfNextMessage(); will( 
                        new NewMessageAvailableDuringGetTimeOfNextMessageAction(
                                messageListenerManager, 150, 200 ) );
                inSequence(sequence);
            }
        });
        
        clock.increaseTime(99);
        executor.tick(99, TimeUnit.MILLISECONDS);
        
        // Check all the calls happened
        context.assertIsSatisfied();
        
        // The future scheduled task should still be there
        assertEquals( 1, executor.getNumScheduled() );
        
        context.checking (new Expectations () {
            {
                @SuppressWarnings("unchecked")
                InflightEntry<AckIdV3, BasicInflightInfo> dequeuedEntry = 
                        context.mock( InflightEntry.class, "Entry1" );
                oneOf(mockMessageSource).dequeue(); will( returnValue( dequeuedEntry ) );
                inSequence(sequence);
                
                oneOf(mockMessageListener).onMessage( dequeuedEntry );
                inSequence(sequence);
                
                oneOf(mockMessageSource).dequeue(); will( returnValue(null ) );
                inSequence(sequence);
                
                oneOf(mockMessageSource).getTimeOfNextMessage(); will( returnValue( Long.MAX_VALUE ) );
                inSequence(sequence);
            }
        });
        
        clock.increaseTime(50);
        executor.tick(50, TimeUnit.MILLISECONDS);
        
        // Check all the calls happened
        context.assertIsSatisfied();
        
        // Nothing should be scheduled
        assertEquals( 0, executor.getNumScheduled() );
    }
    
    @Test
    public void testLaterMessageAvailableDuringGetTimeOfNextMessage() throws SeqStoreException {
        context.checking (new Expectations () {
            {
                oneOf(mockMessageSource).getTimeOfNextMessage(); will( returnValue( 100L ) );
            }
        });
        
        final MessageListenerManager<AckIdV3, BasicInflightInfo> messageListenerManager 
            = new MessageListenerManager<AckIdV3, BasicInflightInfo>(
                clock, mockMessageSource, mockMessageListener, executor);
        
        assertEquals( 1, executor.getNumScheduled() );
        // The message isn't ready yet
        assertTrue( executor.isIdle() );
        
        final Sequence sequence = context.sequence("Sequence");
        
        context.checking (new Expectations () {
            {
                @SuppressWarnings("unchecked")
                InflightEntry<AckIdV3, BasicInflightInfo> dequeuedEntry = 
                        context.mock( InflightEntry.class, "Entry0" );
                oneOf(mockMessageSource).dequeue(); will( returnValue( dequeuedEntry ) );
                inSequence(sequence);
                
                oneOf(mockMessageListener).onMessage( dequeuedEntry );
                inSequence(sequence);
                
                oneOf(mockMessageSource).dequeue(); will( returnValue(null) );
                inSequence(sequence);
                
                oneOf(mockMessageSource).getTimeOfNextMessage(); will( 
                        new NewMessageAvailableDuringGetTimeOfNextMessageAction(
                                messageListenerManager, 200, 150 ) );
                inSequence(sequence);
            }
        });
        
        clock.increaseTime(99);
        executor.tick(99, TimeUnit.MILLISECONDS);
        
        // Check all the calls happened
        context.assertIsSatisfied();
        
        // The future scheduled task should still be there
        assertEquals( 1, executor.getNumScheduled() );
        
        context.checking (new Expectations () {
            {
                @SuppressWarnings("unchecked")
                InflightEntry<AckIdV3, BasicInflightInfo> dequeuedEntry = 
                        context.mock( InflightEntry.class, "Entry1" );
                oneOf(mockMessageSource).dequeue(); will( returnValue( dequeuedEntry ) );
                inSequence(sequence);
                
                oneOf(mockMessageListener).onMessage( dequeuedEntry );
                inSequence(sequence);
                
                oneOf(mockMessageSource).dequeue(); will( returnValue(null ) );
                inSequence(sequence);
                
                oneOf(mockMessageSource).getTimeOfNextMessage(); will( returnValue( Long.MAX_VALUE ) );
                inSequence(sequence);
            }
        });
        
        clock.increaseTime(50);
        executor.tick(50, TimeUnit.MILLISECONDS);
        
        // Check all the calls happened
        context.assertIsSatisfied();
        
        // Nothing should be scheduled
        assertEquals( 0, executor.getNumScheduled() );
    }
    
    @Test
    public void testManagesListener() throws SeqStoreException {
        context.checking (new Expectations () {
            {
                oneOf(mockMessageSource).getTimeOfNextMessage(); will( returnValue( 100L ) );
            }
        });
        
        final MessageListenerManager<AckIdV3, BasicInflightInfo> messageListenerManager 
            = new MessageListenerManager<AckIdV3, BasicInflightInfo>(
                clock, mockMessageSource, mockMessageListener, executor);
        
        assertTrue( messageListenerManager.managesListener(mockMessageListener));
        @SuppressWarnings("unchecked")
        MessageListener<AckIdV3, BasicInflightInfo> otherListener = 
        context.mock(MessageListener.class, "otherListener");
        assertFalse( messageListenerManager.managesListener(otherListener));
    }

    @RequiredArgsConstructor
    private static class TestMessageSource implements MessageSource<AckIdV3, BasicInflightInfo> {
        private static final Random random = new Random();
        
        private final AtomicInteger messagesSent = new AtomicInteger();
        
        private final Clock clock;
        
        @Override
        public String getName() {
            return "TestSource";
        }
        
        public int getMessagesSent() {
            return messagesSent.get();
        }

        @Override
        public long getTimeOfNextMessage() throws SeqStoreException {
            if( random.nextDouble() > 0.9 ) {
                Thread.yield();
            }
            
            double randVal = random.nextDouble();
            if( randVal > 0.2 ) {
                return clock.getCurrentTime() + random.nextInt(400) - 100;
            } else if( randVal > 0.05 ) {
                return Long.MAX_VALUE;
            } else if( randVal > 0.025 ) {
                throw new SeqStoreDatabaseException("TestException");
            } else if( randVal > 0.01 ) {
                throw new RuntimeException("Runtime Exception" );
            } else {
                throw new TestExecutorService.IgnoredError();
            }
        }

        @Override
        public InflightEntry<AckIdV3, BasicInflightInfo> dequeue() throws SeqStoreException {
            if( random.nextDouble() > 0.9 ) {
                Thread.yield();
            }
            
            double randVal = random.nextDouble();
            if( randVal > 0.2 ) {
                messagesSent.incrementAndGet();
                
                return new InflightEntry<AckIdV3, BasicInflightInfo>(
                        new StoredEntryV3( AckIdV3.MINIMUM, new TestEntry("TestEntry") ),
                        new BasicInflightInfo(1), 100l );
            } else if( randVal > 0.05 ) {
                return null;
            } else if( randVal > 0.025 ) {
                throw new SeqStoreDatabaseException("TestException");
            } else if( randVal > 0.01 ) {
                throw new RuntimeException("Runtime Exception" );
            } else {
                throw new TestExecutorService.IgnoredError();
            }
        }
    }
    
    private static class ValidatingMessageListener implements MessageListener<AckIdV3, BasicInflightInfo> {
        private static final Random random = new Random();
        
        private final AtomicInteger activeThreadCount = new AtomicInteger();
        
        private AtomicReference<AssertionError> firstError = new AtomicReference<AssertionError>();
        
        private volatile boolean shutdown;
        
        private int numReceived;
        
        public int getNumReceived() {
            return numReceived;
        }
        
        public boolean isShutdown() {
            return shutdown;
        }
        
        private void reportError(AssertionError e) {
            firstError.compareAndSet(null, e);
            log.error(e.getMessage(), e );
        }
        
        public void rethrowFirstError() {
            AssertionError error = firstError.get();
            if( error != null ) {
                throw error;
            }
        }
        
        @Override
        public void onMessage(InflightEntry<AckIdV3, BasicInflightInfo> entry) {
            onEntry();
            try {
                numReceived++;
                
                if( entry == null ) {
                    reportError( new AssertionError( "Entry was null" ) );
                }
                
                if( random.nextDouble() > 0.9 ) {
                    Thread.yield();
                }
                
                throwRandomError();
            } finally {
                onExit();
            }
        }

        private void throwRandomError() throws Error {
            double randVal = random.nextDouble();
            if( randVal > 0.2 ) {
                return;
            } else if( randVal > 0.1 ) {
                throw new RuntimeException("Runtime Exception" );
            } else {
                throw new TestExecutorService.IgnoredError();
            }
        }
        
        @Override
        public void onException(SeqStoreException exception) {
            onEntry();
            try {
                if( random.nextDouble() > 0.9 ) {
                    Thread.yield();
                }
                
                if( exception == null ) {
                    reportError( new AssertionError( "exception was null" ) );
                }
                
                throwRandomError();
            } finally {
                onExit();
            }
        }
        
        private void onEntry() {
            int activeThreads = activeThreadCount.incrementAndGet();
            if( activeThreads != 1 ) {
                reportError( new AssertionError( "Too many active threads: " + activeThreads ) );
            }
        }
        
        private void onExit() {
            activeThreadCount.decrementAndGet();
        }
        
        @Override
        public void shutdown() {
            shutdown = true;
        }
    }
    
    @RequiredArgsConstructor
    private static class NotifyTask implements Runnable {
        private final MessageListenerManager<AckIdV3, BasicInflightInfo> manager;
        
        private final Clock clock;
        
        private final Random random = new Random();
        
        private final long endTimeNanos;
        
        private final CyclicBarrier startBarrier;
        
        @Override
        public void run() {
            try {
                startBarrier.await(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                fail("interrupted before startup");
            } catch (BrokenBarrierException e) {
                fail("BrokenBarrierException before startup");
            } catch (TimeoutException e) {
                fail("Timeout before startup");
            }
            
            long remaining = endTimeNanos - System.nanoTime();
            while( remaining > 0 ) {
                long timeOfNextMessage = clock.getCurrentTime() + random.nextInt(100) - 50;
                manager.newMessageAvailable(timeOfNextMessage);

                if( random.nextDouble() < 0.8 ) { // Sleep 80% of the time
                    long sleepTime = random.nextLong() %
                            Math.min( remaining, random.nextInt( ( int )TimeUnit.MILLISECONDS.toNanos(20 ) ) );
                    try {
                        TimeUnit.NANOSECONDS.sleep(sleepTime);
                    } catch (InterruptedException e) {
                        fail("interrupted while running");
                    }
                }
                remaining = endTimeNanos - System.nanoTime();
            }
        }
    }
    
    @Test
    public void testConcurrency() throws InterruptedException, ExecutionException, TimeoutException {
        final int numNotifyThreads = 15;
        final long runtimeNanos = TimeUnit.SECONDS.toNanos(30);
        
        // Turn off the logging to avoid flooding the logs
        Logger mlmLogger = Logger.getLogger(MessageListenerManager.class);
        mlmLogger.setAdditivity(false);
        
        try {
            Clock clock = new SystemClock();
            TestMessageSource testSource = new TestMessageSource(clock);
            ValidatingMessageListener listener = new ValidatingMessageListener();
            
            TestScheduledExecutorService testScheduledExecutorService = new 
                    TestScheduledExecutorService("testConcurrency-ScheduledExecutor", 4);
            MessageListenerManager<AckIdV3, BasicInflightInfo> manager = 
                    new MessageListenerManager<AckIdV3, BasicInflightInfo>(
                            clock, testSource, listener, testScheduledExecutorService ); 
            
            
            CyclicBarrier startBarrier = new CyclicBarrier(numNotifyThreads);
    
            long endTime = System.nanoTime() + runtimeNanos;
            TestExecutorService testExecutorService = new TestExecutorService(); 
            for( int i = 0; i < numNotifyThreads; ++i ) {
                testExecutorService.submit( new NotifyTask(manager, clock, endTime, startBarrier) );
            }
            
            testExecutorService.shutdownWithin( (long) ( 1.5 * runtimeNanos ), TimeUnit.NANOSECONDS );
            listener.rethrowFirstError();
            testExecutorService.rethrow();
            
            try {
                manager.shutdown().get(1, TimeUnit.SECONDS);
            } catch( TimeoutException e ) {
                JVMStatusLogger.logAllThreads( log, "testConcurrency deadlocked:" );
                fail( "deadlocked" );
            }
            
            testScheduledExecutorService.rethrow();
            testScheduledExecutorService.shutdownWithin(10, TimeUnit.MILLISECONDS);
            
            assertEquals( testSource.getMessagesSent(), listener.getNumReceived() );
            System.out.println( "Processed " +  listener.getNumReceived() + " messages" );
            assertTrue( listener.isShutdown() );
        } finally {
            mlmLogger.setAdditivity(true);
        }
    }
}
