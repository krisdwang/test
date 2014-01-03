package com.amazon.messaging.seqstore.v3.internalInterface;

import java.util.Set;

import com.amazon.messaging.annotations.TestOnly;
import com.amazon.messaging.seqstore.v3.AckId;
import com.amazon.messaging.seqstore.v3.SeqStore;
import com.amazon.messaging.seqstore.v3.config.SeqStoreConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;

/**
 * A factory for the stores that is used to configure and create stores as well
 * as delete them when they are no longer needed.
 * 
 * @author kaitchuc
 */
public interface SeqStoreManager<IdType extends AckId, InfoType> {
    /**
     * Returns true if the environment is healthy, else false.
     * @return
     */
    boolean isHealthy();
    

    /**
     * Create a new store with a specified configuration.
     * 
     * @param name
     *            client provided destination id
     * @throws SeqStoreException
     */
    SeqStore<IdType, InfoType> createStore(StoreId store) throws SeqStoreException;

    /**
     * Get store for the name. Destination should be created in advance through
     * {@link #configureDestination(String, SeqStoreConfig)}. Each call
     * increments internal counter that should be decremented through {SeqStore
     * {@link #close()}.
     * 
     * @param destinationId
     *            id of a destination
     */
    SeqStore<IdType, InfoType> getStore(StoreId store) throws SeqStoreException;

    /**
     * Remove all persistent state associated to the destination.
     * 
     * @param name
     *            id of a destination to remove.
     */
    void deleteStore(StoreId store) throws SeqStoreException;
    
    /**
     * Remove all persistent state associated to the destination if it has been empty
     * for at least <code>minimumEmptyTime</code> ms.
     * 
     * @param name
     *            id of a destination to remove.
     */
    boolean deleteStoreIfEmpty(StoreId store, long minimumEmptyTime) throws SeqStoreException;

    void closeStore(StoreId store) throws SeqStoreException;

    /**
     * Returns ids of all known stores. This set is backed by the manager and will change
     * to reflect new stores being added.
     */
    Set<StoreId> getStoreNames() throws SeqStoreDatabaseException, SeqStoreClosedException;
    
    /**
     * Returns true if the store exists, else fails
     */
    boolean containsStore(StoreId store) throws SeqStoreDatabaseException, SeqStoreClosedException;
    
    /**
     * Get a snapshot of groups managed by the manager.
     */
    Set<String> getGroups();
    
    /**
     * Flushes all persistent data to the disk, releases all external resources.
     * Calls to any methods of the manager and its parts can lead to unexpected
     * results after this method was called.
     */
    void close() throws SeqStoreException;
    
    /**
     * Prepare for close. Calling this is not required but may 
     * speed up shutdown by allowing long running tasks (e.g. BDB cleanup) to exit 
     * while the rest of the shutdown is running instead of waiting 
     * for the call to close before starting their shutdown.
     */
    void prepareForClose();

    /**
     * Enables Quiescent Mode on all managed SeqStore. <BR>
     * Quiescent mode disables future enqueues on the seqstore until such time
     * that it is cancelled via the {@link disableQuiescentMode}
     * 
     * @param reason
     *            - Reason for enabling Quiescent mode, Will be thrown as the
     *            reason of EnqueuesDisabledException.
     * @see com.amazon.messaging.seqstore.v3.exceptions.EnqueuesDisabledException
     */
    void enableQuiescentMode(String reason) throws SeqStoreException;

    /**
     * Enables Enqueues on all managed SeqStores
     */
    void disableQuiescentMode() throws SeqStoreException;

    /**
     * Dump statistics about the store manager to stdout. This is for debugging
     * purposes only.
     * 
     * @throws SeqStoreException
     */
    @TestOnly
    void printDBStats() throws SeqStoreException;
}
