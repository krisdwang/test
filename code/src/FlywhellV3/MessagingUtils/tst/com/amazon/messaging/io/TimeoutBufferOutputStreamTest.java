package com.amazon.messaging.io;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.Cleanup;

import org.junit.Test;

import com.amazon.messaging.testing.TestCase;
import com.amazon.messaging.utils.Scheduler;


public class TimeoutBufferOutputStreamTest extends TestCase {

    private OutputStream blocking = new OutputStream() {
        
        volatile Thread writing;
        AtomicBoolean closed = new AtomicBoolean(false);
        
        @Override
        public void write(int b) throws IOException {
            try {
                writing = Thread.currentThread();
                Thread.sleep(10000); //Not infinite to allow the test to fail.
            } catch (InterruptedException e) {
                throw new IOException();
            } finally {
                writing = null;
            }
        }
        
        @Override
        public void close(){
            if (closed.compareAndSet(false, true) && writing != null) {
                writing.interrupt();
            }
        }
    };
    
    private ByteArrayOutputStream nonBlocking = new ByteArrayOutputStream();
    
    @Test
    public void testUnblock() throws IOException {
        @Cleanup
        TimeoutBufferOutputStream stream = new TimeoutBufferOutputStream(blocking, 1, Scheduler.getGlobalInstance(), 30);
        long startTime = System.currentTimeMillis();
        try {
            stream.write(new byte[]{1,0});
            //Thread.interrupted();
            fail();
        } catch (IOException e) {
            assertNotNull(e);
        }
        assertTrue(startTime + 30 <= System.currentTimeMillis());
        assertTrue(startTime + 1000 >= System.currentTimeMillis());
    }
    
    @Test
    public void testWrite() throws IOException {
        @Cleanup
        TimeoutBufferOutputStream stream = new TimeoutBufferOutputStream(nonBlocking, 1, Scheduler.getGlobalInstance(), 30);
        long startTime = System.currentTimeMillis();
        byte[] array = new byte[]{1,0};
        stream.write(array);
        assertTrue(startTime + 20 >= System.currentTimeMillis());
        assertTrue(Arrays.equals(nonBlocking.toByteArray(),array));
    }
}
