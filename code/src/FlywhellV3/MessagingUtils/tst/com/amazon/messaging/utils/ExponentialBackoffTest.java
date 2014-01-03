package com.amazon.messaging.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.amazon.messaging.impls.SettableClock;
import com.amazon.messaging.utils.ExponentialBackoff;

public class ExponentialBackoffTest {
    private static final long INITIAL_BACKOFF = 10;
    private static final long MAX_BACKOFF = 1000;
    private static final double MULTIPLIER = 2;
    
    @Test
    public void testExponentialBackoff() {
        SettableClock clock = new SettableClock(1);
        ExponentialBackoff backoff = new ExponentialBackoff(INITIAL_BACKOFF, MAX_BACKOFF, MULTIPLIER, clock );
        
        assertFalse( backoff.waitingToRetry() );
        runBackoffToMax(clock, backoff);
        backoff.resetBackoffInterval();
        clock.increaseTime( backoff.getTimeTillNextTry() );
        runBackoffToMax(clock, backoff);
    }
    
    private void runBackoffToMax(SettableClock clock, ExponentialBackoff backoff) {
        long currentBack0ff = INITIAL_BACKOFF;
        
        int maxBackoffCount = 0; // Times we've reached max backoff
        while( maxBackoffCount <= 2 ) {
            assertTrue( backoff.recordFailure() );
            assertTrue( backoff.waitingToRetry() ); 
            assertEquals( currentBack0ff, backoff.getTimeTillNextTry() );
            assertEquals( clock.getCurrentTime() + currentBack0ff, backoff.getNextTryTime() );
            
            assertFalse( backoff.recordFailure() ); // A second failure in the same period should return false
            
            // Go to right before the next try try time and make sure that new failures don't affect the 
            //  time of the next try.
            clock.increaseTime( currentBack0ff - 1 );
            assertTrue( backoff.waitingToRetry() );
            assertFalse( backoff.recordFailure() ); // An extra failure in the backoff period shouldn't do anything
            assertEquals( 1L, backoff.getTimeTillNextTry() );
            assertEquals( clock.getCurrentTime() + 1, backoff.getNextTryTime() );
            
            // Move to when the next try should occur
            clock.increaseTime( 1 );
            assertFalse( backoff.waitingToRetry() );
            assertEquals( 0L, backoff.getTimeTillNextTry() );
            assertEquals( clock.getCurrentTime(), backoff.getNextTryTime() );
            
            clock.increaseTime( 1 );
            assertEquals( 0L, backoff.getTimeTillNextTry() ); // Make sure the time till next try doesn't go negative
            assertEquals( clock.getCurrentTime() - 1, backoff.getNextTryTime() );
            
            currentBack0ff = Math.min( MAX_BACKOFF, ( long ) ( currentBack0ff * MULTIPLIER ) );
            if( currentBack0ff == MAX_BACKOFF ) maxBackoffCount++;
        }
    }
}
