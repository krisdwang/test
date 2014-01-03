package com.amazon.messaging.seqstore.v3;

import java.util.Set;

import com.amazon.coral.metrics.Metrics;
import com.amazon.messaging.seqstore.v3.config.SeqStoreImmutableConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreMissingConfigException;

import edu.umd.cs.findbugs.annotations.CheckForNull;

/**
 * The main interface for the storage system. It allows for messages to be
 * enqueued and the creation of readers to receive them. Each reader created
 * with a different name will receive an complete and independent set of all of
 * the messages enqueued here. Different stores should be independent and not
 * affect one another.
 * 
 * @author kaitchuc
 * @param <E>
 *            The type of messages that are inserted into the store.
 * @param <IdType>
 *            The type of the AckIds that are provided by this implementation.
 * @param <InfoType>
 *            The type of in-flight info object to be associated with each
 *            message by the user of this implementation.
 */
public interface SeqStore<IdType extends AckId, InfoType> {

    /**
     * Name of the destination of the SeqStore.
     */
    public abstract String getStoreName();
    
    /**
     * Return true if the store is open and capable of taking new messages, false
     * if the store has been deleted or closed. Note that the store can still be 
     * closed or deleted instantly after isOpen returned. If you want to rely on 
     * that not changing external synchronization is needed.
     * 
     * @return
     */
    public abstract boolean isOpen();

    /**
     * Store an entry.
     * 
     * @param message
     *            the message to enqueue
     * @param timeout
     *            how long to spend attempting to enqueue the message before timing out. If -1 the timeout
     *            is treated as infinite 
     * @param metrics an optional metrics object to use to report metrics about the enqueue
     * @throws InterruptedException if the enqueue thread is interrupted waiting for the enqueue to complete 
     * @throws {@link EnqueuesDisabledException} - If store is in
     *         {@link com.amazon.messaging.seqstore.SeqStoreManager#enableQuiescentMode(String)
     *         quiescent mode}
     * @throws {@link EnqueueFailedException} - If an error occurs when
     *         enqueuing the message
     */
    public abstract void enqueue(Entry message, long timeout, Metrics metrics) throws SeqStoreException, InterruptedException;

    /**
     * Number of messages added to the store since it was created in this VM.
     */
    public abstract long getEnqueueCount();

    /**
     * Get a reader with a given id. Reader has to have been previously created using {@link #createReader(String)}.
     * 
     * @param readerId
     *            id of a reader
     * @return the reader or null if the reader does not exist.
     * @throws SeqStoreClosedException 
     * @throws SeqStoreException 
     */
    @CheckForNull
    public abstract SeqStoreReader<IdType, InfoType> getReader(String readerId) throws SeqStoreClosedException, SeqStoreException;

    /**
     * Update this stores config from the configuration provider set on
     * the manager that created it. If the config is not found the store continues running
     * using the old configuration.
     * 
     * @throws SeqStoreMissingConfigException if the config provider does not have config for this store.
     * @throws SeqStoreClosedException 
     * @throws SeqStoreException if a reader had to be created and the reader creation failed
     */
    void updateConfig() throws SeqStoreClosedException, SeqStoreMissingConfigException, SeqStoreException;

    /**
     * Force cleanup of expired messages to run immediately. This function will
     * block until cleanup is complete. This is intended for use mainly by unit
     * tests.
     * @throws SeqStoreClosedException 
     */
    public void runCleanupNow() throws SeqStoreClosedException;

    /**
     * Create or update reader with a given id. Each client provided readerId
     * corresponds to a group of consumers that see this store as a queue. To
     * implement topic like behavior use multiple readers with different ids.
     * 
     * @param readerId
     *            chosen by a client code and should be unique for each group of
     *            consumers on the store.
     * @throws SeqStoreException 
     */
    public abstract SeqStoreReader<IdType, InfoType> createReader(String readerId ) throws SeqStoreException;
    
    /**
     * Removes all persistent state associated with the reader.
     * @throws SeqStoreException 
     */
    public abstract void removeReader(String readerId) throws SeqStoreException;

    /**
     * Returns ids of all known readers.
     * @throws SeqStoreClosedException 
     */
    public abstract Set<String> getReaderNames() throws SeqStoreClosedException;

    /**
     * Enables Quiescent Mode on the store. <BR>
     * Quiescent mode disables future enqueues on the seqstore until such time
     * that it is cancelled via the {@link disableQuiescentMode}
     * 
     * @param reason
     *            - Reason for enabling Quiescent mode, Will be thrown as the
     *            reason of EnqueuesDisabledException.
     * @see com.amazon.messaging.seqstore.v3.exceptions.EnqueuesDisabledException
     */
    public abstract void enableQuiescentMode(String reason);

    /**
     * Enables Enqueues on the store
     */
    public abstract void disableQuiescentMode();
    
    /**
     * Get the metrics for the store. Note these metrics are not a snapshot
     * of a point in time and may not fully represent all enqueues/dequeues
     * going on while getStoreMetrics is running.
     * 
     * @return
     */
    public abstract SeqStoreMetrics getStoreMetrics() throws SeqStoreException;
    
    /**
     * Get the metrics for all active stores. For pure SeqStore this is the 
     * same as getStoreMetrics() but for a Flywheel store this excludes messages
     * that are backed up on the local host for other hosts that the local host
     * sees as available.
     * 
     * Note these metrics are not a snapshot of a point in time and may not fully 
     * represent all enqueues/dequeues going on while getStoreMetrics is running.
     * 
     * @return
     */
    public abstract SeqStoreMetrics getActiveStoreMetrics() throws SeqStoreException;

    /**
     * Return if this store is empty.
     * 
     * @return true if the store is empty, else false.
     * @throws SeqStoreClosedException
     * @throws SeqStoreDatabaseException
     */
    boolean isEmpty() throws SeqStoreClosedException, SeqStoreDatabaseException;
    
    /**
     * Return the current configuration being used by the store.
     */
    SeqStoreImmutableConfig getConfig();
}
