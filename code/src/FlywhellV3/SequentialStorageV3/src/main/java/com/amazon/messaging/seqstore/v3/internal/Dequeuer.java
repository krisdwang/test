package com.amazon.messaging.seqstore.v3.internal;



import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import lombok.Getter;

import net.jcip.annotations.GuardedBy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.messaging.seqstore.v3.InflightEntry;
import com.amazon.messaging.seqstore.v3.InflightEntryInfo;
import com.amazon.messaging.seqstore.v3.InflightInfoFactory;
import com.amazon.messaging.seqstore.v3.MessageListener;
import com.amazon.messaging.seqstore.v3.StoreReader;
import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.config.SeqStoreReaderImmutableConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreInternalException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreLockTimeoutException;
import com.amazon.messaging.seqstore.v3.internalInterface.Inflight;
import com.amazon.messaging.seqstore.v3.store.Store;
import com.amazon.messaging.seqstore.v3.store.StoreSpecificClock;
import com.amazon.messaging.utils.MathUtil;
import com.google.common.util.concurrent.Futures;



/**
 * Retrieves messages from the store and inflight and adds them to the inflight table.
 * 
 * @author stevenso
 */
public class Dequeuer<InfoType> {

    private static final Log log = LogFactory.getLog(Dequeuer.class);
    
    private static final Future<Void> immediateSucceededFuture = Futures.immediateFuture (null);

    // The maximum time to wait for a lock before giving up and throwing
    // SeqStoreLockTimeoutException
    // TODO: Make it configurable
    private static final int MAX_LOCK_TIME_MS = 5000;

    private final Inflight<AckIdV3, InfoType> inflight;

    private final Store store;

    private final Lock storeReaderLock = new ReentrantLock();

    private final StoreReader reader;

    @Getter
    private volatile SeqStoreReaderImmutableConfig<InfoType> config;

    private Object messageListenerManagerLock = new Object();

    @GuardedBy("messageListenerManagerLock")
    private volatile MessageListenerManager<AckIdV3, InfoType> messageListenerManager;
    
    public Dequeuer(StoreSpecificClock clock, StoreReader storeReader, Inflight<AckIdV3, InfoType> inflight,
                    Store store, SeqStoreReaderImmutableConfig<InfoType> config)
    {
        this.reader = storeReader;
        this.inflight = inflight;
        this.store = store;
        this.config = config;
    }

    public InflightEntry<AckIdV3, InfoType> dequeueFromStore() throws SeqStoreException {
        InflightInfoFactory<InfoType> inflightInfoFactory = config.getInflightInfoFactory();
        InfoType infoForDequeue = inflightInfoFactory.getMessageInfoForDequeue(null);
        int timeoutForDequeue = inflightInfoFactory.getTimeoutForDequeue(infoForDequeue);

        try {
            if (!storeReaderLock.tryLock(MAX_LOCK_TIME_MS, TimeUnit.MILLISECONDS)) {
                throw new SeqStoreLockTimeoutException("Timedout waiting for dequeue lock.");
            }
        } catch (InterruptedException e) {
            throw new SeqStoreInternalException("Interrupted waiting for dequeue");
        }

        StoredEntry<AckIdV3> entry;
        try {
            entry = reader.dequeue();

            if (entry == null) {
                return null;
            }

            inflight.add(entry.getAckId(), infoForDequeue, timeoutForDequeue);
        } finally {
            storeReaderLock.unlock();
        }

        onUpdate(timeoutForDequeue);

        return new InflightEntry<AckIdV3, InfoType>(entry, infoForDequeue, timeoutForDequeue);
    }

    public InflightEntry<AckIdV3, InfoType> dequeueFromInflight() throws SeqStoreException {
        InflightEntryInfo<AckIdV3, InfoType> dequeuedEntryInfo;
        StoredEntry<AckIdV3> msg = null;

        InflightInfoFactory<InfoType> inflightInfoFactory = config.getInflightInfoFactory();
        dequeuedEntryInfo = inflight.dequeueMessage(inflightInfoFactory);

        while (dequeuedEntryInfo != null) {
            msg = store.get(dequeuedEntryInfo.getAckId());

            if (msg != null) {
                onDequeue(dequeuedEntryInfo.getDelayUntilNextRedrive());
                return new InflightEntry<AckIdV3, InfoType>(msg, dequeuedEntryInfo);
            }

            log.info("Acking " + dequeuedEntryInfo.getAckId() + " as it has expired from disk.");
            inflight.ack(dequeuedEntryInfo.getAckId()); 

            // Try the next one
            dequeuedEntryInfo = inflight.dequeueMessage(inflightInfoFactory);
        }

        return null;
    }

    /*
     * (non-Javadoc)
     * @see com.amazon.messaging.seqstore.v3.Dequeue#dequeue(long)
     */
    public InflightEntry<AckIdV3, InfoType> dequeue() throws SeqStoreException {
        InflightEntry<AckIdV3, InfoType> entry = dequeueFromInflight();
        if (entry == null)
            entry = dequeueFromStore();
        return entry;
    }

    public long getTimeOfNextStoreMessage() throws SeqStoreException {
        return store.getTimeOfNextMessage(reader.getPosition());
    }

    public long getTimeOfNextInflightMessage() {
        return inflight.getNextRedeliveryTime();
    }

    public long getTimeOfNextMessage() throws SeqStoreException {
        return Math.min(getTimeOfNextStoreMessage(), getTimeOfNextInflightMessage());
    }

    /**
     * Closes the reader once this class will no longer be used. Any further
     * attempts to dequeue or peek will result in errors.
     */
    public void close() {
        reader.close();

        Future<Void> shutdownFuture = null;
        synchronized (messageListenerManagerLock) {
            if (messageListenerManager != null) {
                shutdownFuture = messageListenerManager.shutdown ();
                messageListenerManager = null;
            }
        }

        if (shutdownFuture != null) {
            try {
                shutdownFuture.get ();
            } catch (InterruptedException e) {
                // Ignore interruptions
                //
                Thread.currentThread ().interrupt ();
            } catch (ExecutionException e) {
                // Not much to be done here, so just log and ignore
                //
                log.warn ("Could not wait for the message listener to shut down.", e.getCause ());
            }
        }
    }

    public void advanceTo(AckIdV3 pos) throws SeqStoreDatabaseException, SeqStoreClosedException {
        reader.advanceTo(pos);
    }

    public void messageEnqueued(long availableTime) {
        synchronized (messageListenerManagerLock) {
            if (messageListenerManager != null)
                messageListenerManager.newMessageAvailable(availableTime);
        }
    }

    public void onDequeue(long timeout) {
        onUpdate(timeout);
    }

    public void onUpdate(long timeout) {
        synchronized (messageListenerManagerLock) {
            if (messageListenerManager != null) {
                messageListenerManager.newMessageAvailable(MathUtil.addNoOverflow(
                        inflight.getClock().getCurrentTime(), timeout));
            }
        }
    }

    public void setMessageListener(
        ScheduledExecutorService executor, MessageListener<AckIdV3, InfoType> listener)
    {
        if( listener != null && executor == null ) {
            throw new IllegalArgumentException("executor cannot be null if listener is not null" );
        }
        
        synchronized (messageListenerManagerLock) {
            if( messageListenerManager != null ) {
                messageListenerManager.shutdown();
                messageListenerManager = null;
            }
            
            if( listener != null ) {
                MessageListenerManager.MessageSource<AckIdV3, InfoType> messageSource = 
                    new MessageListenerManager.MessageSource<AckIdV3, InfoType>() {
                        @Override
                        public String getName() {
                            return store.getDestinationId().toString();
                        }
                    
                        @Override
                        public InflightEntry<AckIdV3, InfoType> dequeue() throws SeqStoreException {
                            return Dequeuer.this.dequeue();
                        }
                        
                        @Override
                        public long getTimeOfNextMessage() throws SeqStoreException {
                            return Dequeuer.this.getTimeOfNextMessage();
                        }
                    };
                
                messageListenerManager = new MessageListenerManager<AckIdV3, InfoType>(
                        inflight.getClock(), messageSource, listener, executor);
            }
        }
    }
    
    public Future<Void> removeMessageListener (MessageListener<AckIdV3, InfoType> listener) {
        if (listener == null) throw new NullPointerException ("listener cannot be null");

        Future<Void> shutdownFuture = null;
        synchronized (messageListenerManagerLock) {
            if ((messageListenerManager != null) && messageListenerManager.managesListener (listener)) {
                shutdownFuture = messageListenerManager.shutdown ();
                messageListenerManager = null;
            }
        }

        return (shutdownFuture != null ? shutdownFuture : immediateSucceededFuture);
    }

    public void updateConfig(SeqStoreReaderImmutableConfig<InfoType> cfg) {
        config = cfg;
    }
    
    /**
     * Set if messages should be evicted from the cache after they've been dequeued from 
     * the store. This does not affect if messages should be cached when they are dequeued
     * from inflight. Currently messages dequeued from inflight are always cached
     * as the odds of them being read again is much higher.
     */
    public synchronized void setEvictFromCacheAfterDequeueFromStore(boolean evict) {
        reader.setEvictFromCacheAfterReading(evict);
    }
    
    /**
     * Get if messages should be evicted from the cache after they've been dequeued.
     */
    public synchronized boolean isEvictFromCacheAfterDequeueFromStore() {
        return reader.isEvictFromCacheAfterReading();
    }
}
