package com.amazon.messaging.io;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.messaging.annotations.Blocking;
import com.amazon.messaging.annotations.Thrower;
import com.amazon.messaging.utils.Scheduler;

/**
 * An implementation of BufferedOutputStream that will close itself in the event
 * that it detects a write having taken too long. The check is performed on a
 * periodic basis in the scheduler provided.
 * 
 * @author kaitchuc
 */
@Blocking
@Thrower
public class TimeoutBufferOutputStream extends BufferedOutputStream {

    private static final Log log = LogFactory.getLog(TimeoutBufferOutputStream.class);

    private final AtomicLong writeStartCount = new AtomicLong(0);

    private final AtomicLong writeEndCount = new AtomicLong(0);

    private final AtomicLong lastWriteCount = new AtomicLong(0);

    private final int checkFrequency;

    private final Runnable stalledChecker = new Runnable() {

        @Override
        public void run() {
            long started = writeStartCount.get();
            if (writeEndCount.get() < started && started == lastWriteCount.get()) {
                log.warn("Closing " + this.toString() + " because write blocked for longer than: " +
                         checkFrequency);
                try {
                    closeNoFlush();
                } catch (InterruptedIOException e) {
                    log.info("Close interrupted " + this.toString() + " after blocking for longer than: " +
                            checkFrequency +". Exiting.");
                } catch (IOException e) {
                    log.error("Failed to close " + this.toString() + " after blocking for longer than: " +
                              checkFrequency);
                }
            }
            lastWriteCount.set(started);
        }
    };

    private final Scheduler pool;

    public TimeoutBufferOutputStream(OutputStream out, int size, Scheduler s, int checkFrequency) {
        super(out, size);
        this.pool = s;
        this.checkFrequency = checkFrequency;
        s.executePeriodically(
                "Check for brownout on: " + this.toString(), stalledChecker, checkFrequency, false);
    }

    @Override
    public synchronized void write(int b) throws IOException {
        writeStartCount.incrementAndGet();
        super.write(b);
        writeEndCount.incrementAndGet();
    }

    @Override
    public synchronized void write(byte b[], int off, int len) throws IOException {
        writeStartCount.incrementAndGet();
        super.write(b, off, len);
        writeEndCount.incrementAndGet();
    }

    @Override
    public synchronized void flush() throws IOException {
        writeStartCount.incrementAndGet();
        super.flush();
        writeEndCount.incrementAndGet();
    }
    
    @Override
    public void close() throws IOException {
        pool.cancel(stalledChecker, true, false);
        super.close();
    }
    
    public void closeNoFlush() throws IOException {
        pool.cancel(stalledChecker, true, false);
        out.close();
    }
}
