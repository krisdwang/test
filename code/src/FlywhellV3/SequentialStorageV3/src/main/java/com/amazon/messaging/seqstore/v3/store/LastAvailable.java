package com.amazon.messaging.seqstore.v3.store;

import lombok.Getter;

import net.jcip.annotations.GuardedBy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdSource;
import com.amazon.messaging.seqstore.v3.internal.AckIdSourceFactory;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;

/**
 * This class manages the last available pointer for a store. It should be used
 * for requesting new AckIdVs and for ensuring that enqueues and dequeues ack ranges never
 * overlap. It also provides functions for determining when the next message will become
 * available.
 */
public class LastAvailable {
    private static final Log log = LogFactory.getLog(LastAvailable.class);
    
    private final StoreId storeId;
    
    @GuardedBy("this")
    private final BucketJumper iterator;
    
    @Getter
    private final StoreSpecificClock clock;
    
    private final AckIdSource ackIdSource;

    /**
     * The cache id of the last available record. This may be out of date until {@link #updateAvailable()} is
     * called. May be null if there are no messages available.
     */
    @GuardedBy("this") // Guarded only for writes 
    private volatile AckIdV3 lastAvailable;
    
    /**
     * The next message after the last available message. Will be null if that is not known.
     */
    @GuardedBy("this") // Guarded only for writes
    private volatile AckIdV3 nextMessageAfterLastAvailable;

    public LastAvailable(StoreId storeId, BucketManager bm, AckIdSourceFactory ackIdSourceFactory, StoreSpecificClock clock ) 
        throws SeqStoreDatabaseException
    {
        this.storeId = storeId;
        this.iterator = new BucketJumper( storeId + "-BucketJumper", bm );
        this.ackIdSource = ackIdSourceFactory.createAckIdSource( storeId );
        this.clock = clock;
        this.lastAvailable = null;
    }

    /**
     * Move the last available pointer as far forward as it can safely be moved.
     * 
     * @return the new last available pointer
     * @throws SeqStoreException
     */
    public synchronized AckIdV3 updateAvailable() throws SeqStoreDatabaseException, SeqStoreClosedException {
        AckIdV3 minEnqueueLevel = ackIdSource.getMinEnqueueLevel();
        AckIdV3 newLastAvailable = iterator.advanceTo(minEnqueueLevel);
        
        if( newLastAvailable == null ) {
            // lastAvailable could be non null if the message it points to was deleted since the last call to updateAvailable.
            assert lastAvailable == null || minEnqueueLevel.compareTo( lastAvailable ) > 0 :
                "Last available deleted while enqueues could still happen below it";
            nextMessageAfterLastAvailable = null;
        } else if( !newLastAvailable.equals( lastAvailable ) ) {
            assert lastAvailable == null || newLastAvailable.compareTo( lastAvailable ) >= 0 
                : "Last Available went backwards from " + lastAvailable + " to " + newLastAvailable;
            
            nextMessageAfterLastAvailable = null;
        } else if( nextMessageAfterLastAvailable != null && minEnqueueLevel.compareTo( nextMessageAfterLastAvailable ) >= 0 ) {
            // The enqueue for message in nextMessageAfterLastAvailable must have failed
            //  as newLastAvailable didn't move even though minEnqueueLevel is past
            //  nextMessageAfterLastAvailable
            nextMessageAfterLastAvailable = null;
        }
        
        lastAvailable = newLastAvailable;
        
        return newLastAvailable;
    }

    /**
     * The last record that can safely be dequeued. This function returns a cached value. If 
     * a more accurate value is needed call {@link #updateAvailable()}
     * @return
     */
    public AckIdV3 getLastAvailableAckId() {
        return lastAvailable;
    }

    /**
     * Call this to get the ackId for a message. Every AckId retrieved through this function
     * must be marked as completed using {@link #enqueueFinished(AckIdV3)}
     * @param message
     * @return
     */
    public AckIdV3 requestAckId(long requestedTime) {
        AckIdV3 id = ackIdSource.requestNewId(requestedTime);
        return id;
    }
    
    public boolean recordNewId(AckIdV3 ackId) {
        AckIdV3 tmpLastAvailable = lastAvailable; // Local copy so it doesn't change under us.
        if( (tmpLastAvailable != null) && (tmpLastAvailable.compareTo(ackId) > 0) ) {
            return false;
        }
        
        return ackIdSource.recordNewId(ackId);
    }
    
    /**
     * Mark an enqueues as having finished. This does not mean the enqueue was successful only that 
     * no more work is being done for it.
     * 
     * @param ackId
     */
    public void enqueueFinished(AckIdV3 ackId) {
        if( !ackIdSource.isUnfinished( ackId ) ) {
            log.warn( "enqueueFinished called for ackId that is already finished or was never started.");
            return;
        }
        
        // Get the value of lastAvailable before we call enqueueFinished as it can be changed
        //  by another thread afterwards
        AckIdV3 tmpLastAvailable = lastAvailable;
        
        ackIdSource.enqueueFinished(ackId);
        
        // Do this after calling enqueueFinished so we don't block dequeues
        if( tmpLastAvailable != null && ackId.compareTo( tmpLastAvailable ) < 0 ) {
            throw new IllegalStateException( "Received ackId " + ackId + " that is below lastAvailable " + tmpLastAvailable );
        }
        
        synchronized (this) {
            if( nextMessageAfterLastAvailable != null ) {
                nextMessageAfterLastAvailable = AckIdV3.min( ackId, nextMessageAfterLastAvailable );
            }
        }
    }
    
    /**
     * Get the time (relative to the local clock) the next message will become available, 
     * or a time in the past (or now) if a message is already available.
     * <p>
     * If there are already messages in the past this function is not accurate but 
     * will return the time of the current message if one is provided, or 0 if
     * current is null. This allows getTimeOfNextMessage to advance as the time
     * the messages are available advances which allows ordering of stores roughly
     * by when the next message was enqueued into the store. This is much cheaper
     * than accurately discovering the time while still allowing Flywheel to deliver
     * messages roughtly in the order they were enqueued.
     * 
     * @param current the current level 
     * @return the next the message after current will become available if it is not already
     *  available, if the next message is available it returns some time in the past
     * @throws SeqStoreException
     */
    public long getTimeOfNextMessage(AckIdV3 current) throws SeqStoreException {
        if (areMessagesAvailable(current)) {
            if( current != null ) return getLocalTimeForAckId(current);
            else return 0;
        }
        
        AckIdV3 minimumUncompletedEnqueue = ackIdSource.getMinimumUnfinishedEnqueue();
        AckIdV3 tmpNextMessageAfterLastAvailable = nextMessageAfterLastAvailable;
        if( tmpNextMessageAfterLastAvailable != null ) {
            if( minimumUncompletedEnqueue != null && 
                minimumUncompletedEnqueue.compareTo( tmpNextMessageAfterLastAvailable ) <= 0 ) 
            {
                return Long.MAX_VALUE;
            } else {
                if( tmpNextMessageAfterLastAvailable.getTime() == Long.MAX_VALUE ) {
                    return Long.MAX_VALUE;
                } else {
                    return getLocalTimeForAckId(tmpNextMessageAfterLastAvailable);
                }
            }
        }
        
        // Hold the synchronized block as this needs the state to be consistent the entire way through
        synchronized( this ) {
            // Try and move last available forward
            updateAvailable();
            
            // Recheck against lastAvailable now that it may have moved.
            if (areMessagesAvailable(current)) {
                if( current != null ) return getLocalTimeForAckId(current);
                else return 0;
            }
            
            updateNextMessageTime();
        
            minimumUncompletedEnqueue = ackIdSource.getMinimumUnfinishedEnqueue();
            if( minimumUncompletedEnqueue != null && 
                minimumUncompletedEnqueue.compareTo( nextMessageAfterLastAvailable ) <= 0 ) 
            {
                return Long.MAX_VALUE;
            } else {
                return clock.convertStoreTimeToLocalTime( storeId, nextMessageAfterLastAvailable.getTime() );
            }
        }
    }

    public long getLocalTimeForAckId(AckIdV3 current) {
        return clock.convertStoreTimeToLocalTime( storeId, current.getTime() );
    }

    /**
     * Update nextAvailableMessageTime to the time of the first message after lastAvailable. 
     * 
     * @throws SeqStoreException
     */
    private synchronized void updateNextMessageTime() throws SeqStoreException {
        // If the nextMessageAfterLastAvailable is known there is nothing to do
        if( nextMessageAfterLastAvailable != null ) return; 
        
        StoredEntry<AckIdV3> entry = null;
        BucketJumper tmpIterator = iterator.copy();
        try {
            entry = tmpIterator.next();
            // if nothing in store
            if (entry == null) {
                nextMessageAfterLastAvailable = AckIdV3.MAXIMUM;
            } else {
                nextMessageAfterLastAvailable = entry.getAckId();
            }
        } finally {
            tmpIterator.close();
        }
    }

    /**
     * Return the position of lastAvailable within the store.
     * @return
     */
    public StorePosition getPosition() throws SeqStoreDatabaseException {
        return iterator.getPosition();
    }

    private boolean areMessagesAvailable(AckIdV3 current) {
        AckIdV3 tmpLastAvailable = lastAvailable; // Local copy so it doesn't change under us.
        if (tmpLastAvailable != null ) {
            if ((current == null) || (tmpLastAvailable.compareTo(current) > 0)) {
                return true;
            }
            if (current.compareTo(tmpLastAvailable) < 0) {
                throw new IllegalStateException("Sequence went backwards.");
            }
        }
        return false;
    }

    public long getCurrentStoreTime() {
        return clock.getStoreTime(storeId);
    }
    
    public AckIdV3 getMinEnqueueLevel() {
        return ackIdSource.getMinEnqueueLevel();
    }
    
    public boolean hasUnfinishedEnqueue() {
        return ackIdSource.hasUnfinishedEnqueue();
    }

    public AckIdSource getAckIdSource() {
        return ackIdSource;
    }
}
