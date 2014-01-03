package com.amazon.messaging.seqstore.v3;



import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import com.amazon.messaging.seqstore.v3.config.SeqStoreReaderImmutableConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreMaxInflightSizeReachedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreMissingConfigException;

import edu.umd.cs.findbugs.annotations.CheckForNull;



/**
 * High level API of Sequential Storage Consumer.
 */
public interface SeqStoreReader<IdType extends AckId, InfoType> {

    String getReaderName();

    /**
     * Returns a message from the store if one is available. Note that this does not 
     * guarantee not false empty returns
     * 
     * @return the entry for a message or null if no message is available
     */
    InflightEntry<IdType, InfoType> dequeue() throws SeqStoreException, SeqStoreMaxInflightSizeReachedException;
    
    /**
     * Return the times that the next message available for the reader will be available.
     * <p> 
     * Note that this may change when backups get activated, enqueues happen or
     * {@link #update(AckId, int, Object, Object)} gets called.
     * <p>
     * Note also that this is when the inflight limit is reached this will be when
     * the next message is available from inflight and won't count messages from the
     * store until a message is acked. 
     *  
     * @throws SeqStoreException 
     */
    public long getTimeOfNextMessage() throws SeqStoreException;
    
    /**
     * Get the metrics about messages from this reader that are in flight. This runs
     * in O(log n) time where n is the number timedout messages.
     *   
     * @return In-flight metrics for this reader. 
     * @throws SeqStoreException 
     */
    InflightMetrics getInflightMetrics() throws SeqStoreException;

    /**
     * Get a simple count of the number of messages in the in-flight table for
     * this reader. This runs in constant time for a single store.
     * 
     * @return Number of messages in this reader's in-flight table.
     * @throws SeqStoreException 
     */
    int getInflightMessageCount() throws SeqStoreClosedException, SeqStoreException;

    /**
     * Get the metrics about the messages for this reader that have yet to be
     * dequeued from the store. Note these metrics are not a snapshot
     * of a point in time and may not fully represent all enqueues/dequeues
     * going on while getStoreBacklogMetrics is running.
     * 
     * @return
     * @throws SeqStoreException 
     */
    SeqStoreReaderMetrics getStoreBacklogMetrics() throws SeqStoreException;

    /**
     * Retrieve payload by ackId for a dequeued but non acked and non expired
     * message. If the message has not been dequeued or has already been acked
     * and removed this will return null.
     * @throws SeqStoreException 
     */
    @CheckForNull
    InflightEntry<IdType, InfoType> getInFlightMessage(IdType ackId) throws SeqStoreException;

    /**
     * Ack individual message. Only in-flight entries (ones that where retrieved
     * using {@link #dequeue(long)} call during process lifetime) can be acked.
     * As a result of this operation ackLevel might be advanced.
     * 
     * @param ackId
     *            value of {@link StoredEntry#getAckId()}
     * @return true if message with a given ackId was successfully acked. false
     *         if the message was no longer in-flight and therefore could not be
     *         acked. The message will not be inflight if 1)it was acked earlier
     *         2)it was nacked earlier 3)the reader was closed without
     *         checkpointing/restoring
     * @throws SeqStoreException 
     */
    boolean ack(IdType ackId) throws SeqStoreException;

    /**
     * Get information about an in-flight entry without loading the message
     * payload from the persistence layer.
     * 
     * @param ackId
     *     AckId to look up message info for.
     * @return
     *     Information about the in-flight message, or <code>null</code> if the
     *     message was not in flight, because it was never dequeued or already
     *     acknowledged.
     * @throws SeqStoreClosedException
     * @throws SeqStoreException 
     */
    InflightEntryInfo<IdType, InfoType> getInFlightInfo(IdType ackId) throws SeqStoreClosedException, SeqStoreException;

    /**
     * Perform the update requested in updateRequest on the message identified
     * by ackId. If successful this returns {@link InflightUpdateResult#DONE DONE}. 
     * If the update cannot be performed because the expectations aren't met 
     * it returns {@link InflightUpdateResult#EXPECTATION_UNMET EXPECTATION_UNMET}.
     * If the message can't be found it returns  {@link InflightUpdateResult#NOT_FOUND NOT_FOUND}.
     * 
     * @param ackId
     *     Identifies the in-flight message to update.
     * @param updateRequest
     *     The update request containing the information about the requested update
     * @return
     *     {@link InflightUpdateResult result} of the update. 
     * @throws SeqStoreClosedException
     */
    InflightUpdateResult update(IdType ackId, InflightUpdateRequest<InfoType> updateRequest)
    throws SeqStoreException;

    /**
     * Fetch a list containing a snapshot of the information stored for all
     * messages in flight. This can be used to apply an update to all messages
     * in flight.
     * 
     * @return Snapshot of the information stored for all messages in flight.
     * @throws SeqStoreClosedException
     * @throws SeqStoreException 
     */
    List<InflightEntryInfo<IdType, InfoType>> getAllMessagesInFlight() 
        throws SeqStoreClosedException, SeqStoreException;
    
    /**
     * Update this destinations config from the config provider. If the configuration
     * for the reader is missing the reader will continue with the old configuration.
     * 
     * @throws SeqStoreClosedException 
     * @throws SeqStoreMissingConfigException if the configuration for the reader is missing.
     */
    void updateConfig() throws SeqStoreClosedException, SeqStoreMissingConfigException;

    /**
     * Return the current config being used for the reader.
     * @return
     * @throws SeqStoreClosedException 
     */
    SeqStoreReaderImmutableConfig<InfoType> getConfig() throws SeqStoreClosedException;

    /**
     * Get a checkpoint for the reader. The checkpoint can be used with a {@link CheckpointProvider} 
     * to restore the readers state after a restart.
     * <p>
     * Note: Actions taken by other threads while the checkpoint is being created may not be represented
     * in the checkpoint. 
     * 
     * @return a checkpoint 
     * @throws SeqStoreException
     */
    Checkpoint<IdType, ? extends AckId, InfoType> getCheckpoint() throws SeqStoreException;
    
    /**
     * Set a message listener for the reader. If there is an existing message listener already set, it will be
     * overwritten.
     * <p>
     * This method does not synchronize with the completion of the existing message listener. I.e., one notification may
     * still be delivered to the existing listener after this call returns.
     * 
     * @param executor an executor to be used to execute the callbacks
     * @param listener the call back to send the messages to
     * @throws SeqStoreClosedException If the store has been deleted.
     */
    public void setMessageListener (ScheduledExecutorService executor, MessageListener<IdType, InfoType> listener)
            throws SeqStoreClosedException;

    /**
     * Removes a previously installed message listener, only if it is the current listener for that reader and if the
     * store is not already closed. Otherwise returns a completed {@link Future} and has no effect.
     * <p>
     * This method is safe to be called from within a {@link MessageListener} notification.
     * 
     * @param listener listener which should be removed.
     * @return Future, which can be waited on for removal to complete. It is guaranteed that after a returned future
     *         completes, no more messages will be delivered to the listener, which was just removed and its
     *         {@link MessageListener#shutdown()} call would have been invoked.
     */
    public Future<Void> removeMessageListener (MessageListener<IdType, InfoType> listener);
}
