package com.amazon.messaging.seqstore.v3.internalInterface;

import java.util.List;
import java.util.NavigableSet;
import java.util.Set;

import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.v3.AckId;
import com.amazon.messaging.seqstore.v3.Checkpoint;
import com.amazon.messaging.seqstore.v3.InflightEntryInfo;
import com.amazon.messaging.seqstore.v3.InflightInfoFactory;
import com.amazon.messaging.seqstore.v3.InflightMetrics;
import com.amazon.messaging.seqstore.v3.InflightUpdateRequest;
import com.amazon.messaging.seqstore.v3.InflightUpdateResult;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.ReaderInflightData;

import edu.umd.cs.findbugs.annotations.CheckForNull;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Manages the status of the in-flight messages. Implementations need to track both which messages
 * are inflight and when they are scheduled to be next redelivered.
 * 
 * @param <InfoType>
 */
public interface Inflight<IdType extends AckId, InfoType> {

    /**
     * Returns the message info for a message in flight, or <code>null</code>
     * if the message was never or is no longer in flight.
     * 
     * @param ackId ackId of the message to return info for.
     * @param readerStore store to get in-flight info for.
     * @return the message info, or <code>null</code> if the message in not in
     *         flight.
     */
    @CheckForNull
    InflightEntryInfo<IdType, InfoType> getInflightEntryInfo(IdType ackId);

    /**
     * Peek at the first message currently in flight for the given reader that
     * has a next re-delivery time in the past.
     * 
     * @returns
     *   The ackId and info for the first in-flight message with a
     *   re-delivery time in the past for the given reader, or <code>null</code>
     *   if there is no such message.
     */
    @CheckForNull
    InflightEntryInfo<IdType, InfoType> peekNextRedeliverable();

    /**
     * Gets a snapshot of the current in-flight metrics for a store.
     * 
     * @return Inflight metrics for a store.
     */
    @NonNull
    InflightMetrics getInflightMetrics();

    /**
     * Gets the number of messages in the in-flight table for a store,
     * including both messages that are currently ready for re-delivery as well
     * as messages that will be available for re-delivery in the future.
     * 
     * @return Number of messages in the in-flight table for a store.
     */
    int getInflightMessageCount();

    /**
     * Get the ack level for the reader.
     * 
     * @return If messages are inflight, this returns the lowest AckIdV3 for the
     *         store If they are no longer inflight, it returns the greatest
     *         ever acked. If the reader is unknown (there have been no 
     *         ack level updates or acks or dequeues) returns the lowest possible
     *         ack id.
     */
    @NonNull
    IdType getAckLevel();

    /**
     * Clears the inflight table for the given store up to the provided ack
     * level.
     * 
     * @return the number of messages removed from the inflight table by the ack level change
     */
    long setAckLevel(IdType level);

    /**
     * Return the earliest absolute time (milliseconds since the epoch) that a
     * message in flight for the provided reader may be available to be
     * dequeued and re-delivered. If there are no messages in flight waiting
     * to be re-delivered, this method returns {@link Long#MAX_VALUE}.
     *
     * @return Earliest time when a message may be ready to be re-delivered
     *         from the set of messages in flight for this reader, or
     *         {@link Long#MAX_VALUE} if there are no messages in flight
     *         waiting to be re-delivered.
     */
    long getNextRedeliveryTime();

    /**
     * Fetch a list containing a snapshot of the information stored for all
     * messages in flight for a particular reader. This can be used to apply
     * an update to all messages in flight.
     * 
     * @return Snapshot of the information stored for all messages in flight.
     * @throws SeqStoreClosedException
     */
    List<InflightEntryInfo<IdType, InfoType>> getAllMessagesInFlight();

    /**
     * @param reader
     * Nack all messages for the provided reader without incrementing their delivery counts.
     */
    void makeAllDeliverable();

    /**
     * Adds a message to in-flight for the given reader. The message's initial
     * info and timeout is provided by the in-flight info factory.
     * 
     * @param reader reader the message is for.
     * @param ackId ackId of the message to add.
     * @param inflightInfo in-flight message info to associate with the message.
     * @param timeout re-delivery timeout to use for the message.
     * @throws IllegalStateException if the given ackId is already in flight.
     */
    void add(IdType ackId, InfoType inflightInfo, int timeout)
            throws IllegalStateException;

    /**
     * Dequeue the first available message for the given reader. The message's
     * updated info and timeout is provided by the in-flight info factory.
     * 
     * @param reader the reader the message is from
     * @param infoFactory factory used to compute the updated in-flight info
     *        and re-delivery timeout.
     * @return The first available message for the reader, or <code>null</code>
     *         if there are no timed out messages for the reader.
     */
    @CheckForNull
    InflightEntryInfo<IdType, InfoType> dequeueMessage(InflightInfoFactory<InfoType> infoFactory);

    /**
     * Acknowledge the given ack id, removing it from inflight and potentially
     * updating the ack level.
     * 
     * @param reader reader to ack the message for.
     * @param ackId ackId of the message to ack.
     * @return <code>true</code> if the message was in flight and is now acked,
     *         or <code>false</code> if the message was never or is not longer
     *         in flight.
     */
    boolean ack(IdType ackId);

    /**
     * Perform the update requested in updateRequest on the message identified
     * by ackId. If successful this returns {@link InflightUpdateResult#DONE DONE}. 
     * If the update cannot be performed because the expectations aren't met 
     * it returns {@link InflightUpdateResult#DONE DONE}. If the message can't
     * be found it returns  {@link InflightUpdateResult#NOT_FOUND NOT_FOUND}.
     * 
     * @param ackId ackId of the message to update.
     * @param updateRequest the update request describing the update to do 
     * @return the result of the update.
     */
    InflightUpdateResult update(IdType ackId, InflightUpdateRequest<InfoType> updateRequest);

    /**
     * Merge the provided set of messages with the inflight set for the given reader. See 
     * {@link ReaderInflightData#mergePartialCheckpoint(Set, AckIdV3, AckIdV3, long) } for 
     * details on how the inflight and the checkpoint are merged
     * 
     * @param reader reader
     * @param messages A set of AckIds of inflight messages to be merged with own in flight message info.
     * @param checkpointAckLevel the ack level of the checkpoint
     * @param checkpointReadLevel the read level of the checkpoint
     * @return boolean true if there are message that the reader knows are acked that the checkpoint
     *   has inflight.
     *   
     * @see {@link ReaderInflightData#mergePartialCheckpoint(Set, AckIdV3, AckIdV3, long) }
     */
    boolean mergePartialCheckpoint(Set<AckIdV3> messages, 
                                   AckIdV3 checkpointAckLevel, AckIdV3 checkpointReadLevel);
    
    /**
     * Get a checkpoint for the given reader.
     * 
     * @param reader the reader to get the checkpoint for
     * @param maximumInflightMessages the maximum number of inflight messages to include in the checkpoint.
     *   If not all messages are included in the checkpoint the read level of the checkpoint will be adjusted
     *   so that restoring from the checkpoint will not miss any unacked messages. If this has a value less
     *   than 0 then all messages will be included in the checkpoint.   
     * @return the checkpoint for the given reader.
     */
    Checkpoint<AckIdV3, AckIdV3, InfoType> getCheckpoint( int maximumInflightMessages );

    /**
     * Get the clock used for managing message re-delivery.
     * 
     * @return Clock used to calculate re-delivery times.
     */
    Clock getClock();

    /**
     * Returns an unmodifiable sorted set (by id) of the ids of the inflight messages for
     * the given reader.
     */
    NavigableSet<AckIdV3> getAllInflightIds();

    /**
     * Get the read level for the given reader. That is the AckId of the highest message
     * ever inflight for the reader.
     * 
     * @return the read level for the reader
     */
    AckIdV3 getReadLevel();
}
