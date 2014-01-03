package com.amazon.messaging.utils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import static org.junit.Assert.*;

import com.amazon.messaging.utils.BackgroundService;


public class BackgroundServiceTest {

    @Test
    public void testBackgroundService() throws InterruptedException {
        final CountDownLatch startLatch = new CountDownLatch( 1 );
        final CountDownLatch finishLatch = new CountDownLatch( 1 );
        
        BackgroundService testService = new BackgroundService("TestShutdownService", false ) {
            @Override
            protected void run() {
                startLatch.countDown();
                while( !shouldStop() ) {
                    try {
                        assertTrue( isCurrentThreadServiceThread() );
                        Thread.sleep( 5000 );
                    } catch (InterruptedException e) {
                        // Do nothing
                    }
                }
                finishLatch.countDown();
            }
        };
        
        assertFalse( testService.isCritical() );
        assertEquals( "TestShutdownService", testService.getName() );
        assertFalse( testService.isRunning() );
        long startTime = System.currentTimeMillis();
        assertTrue( testService.waitForShutdown( 1 ) );
        // waitForShutdown shouldn't wait at all since the thread is done but another thread might have preempted this 
        // one for a little bit
        assertTrue( System.currentTimeMillis() - startTime < 100 ); 
        
        testService.start();
        assertTrue( testService.isRunning() );
        
        try {
            testService.start();
            fail( "Double start should fail");
        } catch( IllegalThreadStateException e ) {
            // Success
        }
        
        assertTrue( startLatch.await( 1, TimeUnit.SECONDS) );

        assertFalse( testService.waitForShutdown( 100 ) );
        // Make sure its still running
        assertTrue( testService.isRunning() );
        
        testService.requestStop( true );
        assertTrue( testService.shouldStop() );
        assertTrue( finishLatch.await( 1, TimeUnit.SECONDS ) );
        assertTrue( testService.waitForShutdown( 1000 ) );
        assertFalse( testService.isRunning() );
        
        // An extra stop shouldn't be a problem
        assertTrue( testService.stop( 100, true ) );
    }
}
