package com.amazon.messaging.seqstore.v3;

/**
 * An interface used to provide checkpoints to be used to restore inflight state after a system restart.
 *  
 * @author stevenso
 *
 * @param <StoreIdType>
 * @param <AckIdType>
 * @param <InfoType>
 */
public interface CheckpointProvider<StoreIdType, AckIdType extends AckId, LevelType extends AckId, InfoType> {
    /**
     * Get the checkpoint for a given reader. It is safe to assume this will only be called once
     * per restart for a given store and reader. 
     * 
     * @param storeId the id of the store the reader is on
     * @param readerName the name of the reader on that store
     * @return the checkpoint to be used to restore the reader, or null if there is no saved checkpoint.
     */
    public Checkpoint<AckIdType, LevelType, InfoType> getCheckpointForReader( StoreIdType storeId, String readerName );
    
    /**
     * Call to inform the checkpoint provided that a reader has been deleted and so any checkpoint
     * stored for the reader can be dropped.
     * 
     * @param storeId
     * @param readerName
     */
    public void readerDeleted( StoreIdType storeId, String readerName);
}
