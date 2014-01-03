package com.amazon.messaging.seqstore.v3.internal;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.coral.metrics.Metrics;
import com.amazon.messaging.annotations.TestOnly;
import com.amazon.messaging.seqstore.v3.Entry;
import com.amazon.messaging.seqstore.v3.SeqStoreMetrics;
import com.amazon.messaging.seqstore.v3.config.SeqStoreImmutableConfig;
import com.amazon.messaging.seqstore.v3.exceptions.EnqueuesDisabledException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreInternalInterface;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreReaderV3InternalInterface;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.google.common.base.Preconditions;

/**
 * This intercepter is responsible for ensuring mutual exclusion of a thread
 * disposing a store from all threads invoking other methods on it.
 * 
 * @author poojag
 */
class TopDisposableStore<InfoType> implements SeqStoreInternalInterface<InfoType> {

    private final SeqStoreV3<InfoType> next_;

    private static final Log log = LogFactory.getLog(TopDisposableStore.class);

    private final DisposalCoordinator dc_ = new DisposalCoordinator();

    private volatile boolean isDisposed_ = false;

    public TopDisposableStore(SeqStoreV3<InfoType> next) {
        Preconditions.checkNotNull(next);
        next_ = next;
    }

    @Override
    public final void enqueue(Entry message, long timeout, Metrics metrics) throws SeqStoreException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot enqueue to a disposed store " + getStoreId());
            }
            next_.enqueue(message, timeout, metrics);
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }

    /**
     * @throws SeqStoreClosedException 
     * @see com.amazon.messaging.seqstore.v3.SeqStore#getReader(java.lang.String)
     */
    @Override
    public final SeqStoreReaderV3InternalInterface<InfoType> getReader(String readerId) throws SeqStoreClosedException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot getReader for a disposed store "
                        + getStoreId());
            }
            return next_.getReader(readerId);
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }

    /**
     * @throws SeqStoreException 
     * @see com.amazon.messaging.seqstore.v3.SeqStore#createReader(java.lang.String)
     */
    @Override
    public SeqStoreReaderV3InternalInterface<InfoType> createReader(String readerId) throws SeqStoreException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot configureReader for a disposed store "
                        + getStoreId());
            }
            return next_.createReader(readerId);
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }

    /**
     * @see com.amazon.messaging.seqstore.v3.SeqStore#removeReader(java.lang.String)
     */
    @Override
    public final void removeReader(String readerId) throws SeqStoreException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot removeReader from a disposed store "
                        + getStoreId());
            }
            next_.removeReader(readerId);
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }

    /**
     * @throws SeqStoreClosedException 
     * @see com.amazon.messaging.seqstore.v3.SeqStore#getReaderNames()
     */
    @Override
    public final Set<String> getReaderNames() throws SeqStoreClosedException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot getReaderIds for a disposed store "
                        + getStoreId());
            }
            return next_.getReaderNames();
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }

    public final void close() throws SeqStoreException {
        dc_.waitToDispose();
        try {
            if (isDisposed()) {
                if (log.isDebugEnabled()) {
                    log.debug("Already disposed or closed store " + getStoreId() );
                }
                return;
            }
            next_.close();
            markAsDisposed();
        } finally {
            dc_.wakupThreadsAfterDisposal();
        }
    }

    @Override
    public boolean closeIfEmpty(long minimumEmptyTime) throws SeqStoreException {
        dc_.waitToDispose();
        try {
            if(isDisposed()) {
                if(log.isDebugEnabled()) {
                    log.debug("Already disposed store or closed "+getStoreId());
                }
                return true;
            }
            if( next_.closeIfEmpty( minimumEmptyTime ) ) {
                markAsDisposed();
                return true;
            }
            return false;
        } finally {
            dc_.wakupThreadsAfterDisposal();
        }
    }
    
    private final boolean isDisposed() {
        return isDisposed_;
    }

    private final void markAsDisposed() {
        isDisposed_ = true;
    }
    
    // ////////////////////////////////////////////////////////////////////////
    // The methods below do not access or modified state that can be disposed
    // ///////////////////////////////////////////////////////////////////////

    /**
     * @see com.amazon.messaging.seqstore.v3.SeqStore#getStoreName()
     */
    @Override
    @TestOnly
    public final String getStoreName() {
        // this method does not access or modify state that can be disposed
        return next_.getStoreName();
    }
    
    /**
     * @see com.amazon.messaging.seqstore.v3.SeqStore#getEnqueueCount()
     */
    @Override
    public final long getEnqueueCount() {
        return next_.getEnqueueCount();
    }


    @Override
    public void enableQuiescentMode(String reason) {
        next_.enableQuiescentMode(reason);
    }

    @Override
    public void disableQuiescentMode() {
        next_.disableQuiescentMode();
    }

    @Override
    public void updateConfig() throws SeqStoreException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot update config for a disposed store "
                        + getStoreId());
            }
            next_.updateConfig();
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }
    
    @Override
    public void updateConfig(SeqStoreImmutableConfig config, Set<String> requiredReaders) throws SeqStoreException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot update config for a disposed store "
                        + getStoreId());
            }
            next_.updateConfig(config, requiredReaders);
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }

    @Override
    public void runCleanupNow() throws SeqStoreClosedException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot run cleanup for a disposed store "
                        + getStoreId());
            }
            next_.runCleanupNow();
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }

    @Override
    public StoreId getStoreId() {
        // this method does not access or modify state that can be disposed
        return next_.getStoreId();
    }

    @Override
    public String getConfigKey() {
        // this method does not access or modify state that can be disposed
        return next_.getConfigKey();
    }
    
    @Override
    public SeqStoreImmutableConfig getConfig() {
        // this method does not access or modify state that can be disposed
        return next_.getConfig();
    }
    
    @Override
    public boolean isOpen() {
        return next_.isOpen();
    }

    @Override
    public AckIdV3 getMinAvailableLevel() throws SeqStoreClosedException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot run cleanup for a disposed store "
                        + getStoreId());
            }
            return next_.getMinAvailableLevel();
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }
    
    
    @Override
    public SeqStoreMetrics getStoreMetrics() throws SeqStoreException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot run cleanup for a disposed store "
                        + getStoreId());
            }
            return next_.getStoreMetrics();
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }
    
    
    @Override
    public SeqStoreMetrics getActiveStoreMetrics() throws SeqStoreException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot run cleanup for a disposed store "
                        + getStoreId());
            }
            return next_.getActiveStoreMetrics();
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }
    
    @Override
    public long getNumBuckets() throws SeqStoreClosedException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot get the number of buckets for a disposed store "
                        + getStoreId());
            }
            return next_.getNumBuckets();
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }
    
    @Override
    public long getNumDedicatedBuckets() throws SeqStoreClosedException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot get the number of buckets for a disposed store "
                        + getStoreId());
            }
            return next_.getNumDedicatedBuckets();
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }
    
    @Override
    public Object getMBean() throws SeqStoreClosedException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot get the mbean for a disposed store "
                        + getStoreId());
            }
            return next_.getMBean();
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }
    
    @Override
    public void enqueue(AckIdV3 ackId, Entry message, long timeout, boolean cache, Metrics metrics) throws SeqStoreException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot enqueue to a disposed store "
                        + getStoreId());
            }
            next_.enqueue(ackId, message, timeout, cache, metrics);
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }
    
    @Override
    public AckIdV3 getAckIdForEnqueue(long requestedTime) throws EnqueuesDisabledException, SeqStoreClosedException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot get an AckId for enqueue from a disposed store "
                        + getStoreId());
            }
            return next_.getAckIdForEnqueue(requestedTime);
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }
    
    @Override
    public void enqueueFinished(AckIdV3 ackId) throws SeqStoreClosedException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot finish an enqueue for a disposed store "
                        + getStoreId());
            }
            next_.enqueueFinished(ackId);
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }
    
    @Override
    public AckIdV3 getMinEnqueueLevel() throws SeqStoreClosedException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot get the minimum enqueue level for a disposed store "
                        + getStoreId());
            }
            return next_.getMinEnqueueLevel();
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }
    
    @Override
    public AckIdSource getAckIdSource() throws SeqStoreClosedException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot get the ack id source for a disposed store "
                        + getStoreId());
            }
            return next_.getAckIdSource();
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }

    @Override
    public boolean isEmpty() throws SeqStoreClosedException, SeqStoreDatabaseException {
        dc_.waitToExecuteNonDisposalCall();
        try {
            if (isDisposed()) {
                throw new SeqStoreClosedException("Cannot get the minimum enqueue level for a disposed store "
                        + getStoreId());
            }
            return next_.isEmpty();
        } finally {
            dc_.wakeupThreadsAfterNonDisposalCall();
        }
    }
    
    @Override
    public boolean isCacheEnqueues() {
        return next_.isCacheEnqueues();
    }
}
