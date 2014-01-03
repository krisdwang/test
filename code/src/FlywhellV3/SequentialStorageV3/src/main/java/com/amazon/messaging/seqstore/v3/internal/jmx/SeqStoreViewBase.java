package com.amazon.messaging.seqstore.v3.internal.jmx;

import java.io.IOException;

import com.amazon.messaging.seqstore.v3.SeqStore;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;

public class SeqStoreViewBase {

    protected final SeqStore<?, ?> store;

    public SeqStoreViewBase(SeqStore<?, ?> store) {
        this.store = store;
    }

    public long getAvailableMessageCount() throws IOException {
        try {
            return store.getStoreMetrics().getAvailableEntryCount();
        } catch (SeqStoreException e) {
            throw new IOException( e.getMessage(), e );
        }
    }

    public long getDelayedMessageCount() throws IOException {
        try {
            return store.getStoreMetrics().getNumDelayed();
        } catch (SeqStoreException e) {
            throw new IOException( e.getMessage(), e );
        }
    }

    public long getMessageCount() throws IOException {
        try {
            return store.getStoreMetrics().getTotalMessageCount().getEntryCount();
        } catch (SeqStoreException e) {
            throw new IOException( e.getMessage(), e );
        }
    }

    public long getOldestMessageLogicalAge() throws IOException {
        try {
            return store.getStoreMetrics().getOldestMessageLogicalAge();
        } catch (SeqStoreException e) {
            throw new IOException( e.getMessage(), e );
        }
    }

    public long getStoredSize() throws IOException {
        try {
            return store.getStoreMetrics().getTotalMessageCount().getRetainedBytesCount();
        } catch (SeqStoreException e) {
            throw new IOException( e.getMessage(), e );
        }
    }

    public long getEnqueueCount() throws IOException {
        return store.getEnqueueCount();
    }

    protected SeqStore<?, ?> getStore() {
        return store;
    }
}
