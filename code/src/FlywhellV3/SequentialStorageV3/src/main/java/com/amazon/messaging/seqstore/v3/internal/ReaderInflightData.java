package com.amazon.messaging.seqstore.v3.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import net.jcip.annotations.NotThreadSafe;

import lombok.Data;

import com.amazon.messaging.seqstore.v3.Checkpoint;
import com.amazon.messaging.seqstore.v3.CheckpointEntryInfo;
import com.amazon.messaging.seqstore.v3.InflightMetrics;
import com.amazon.messaging.seqstore.v3.InflightUpdateRequest;
import com.amazon.messaging.seqstore.v3.InflightUpdateResult;
import com.amazon.messaging.utils.MathUtil;
import com.amazon.messaging.utils.Pair;
import com.amazon.messaging.utils.collections.BinarySearchSet;

import edu.umd.cs.findbugs.annotations.CheckForNull;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Implements the storage for per reader inflight data.
 * <p>
 * While there are messages in flight it tracks all messages in flight using
 * {@link #ackIdMap} and when they are next scheduled to be re-delivered using
 * {@link #redeliveryScheduleMap}.
 * <p>
 * When there are no messages in flight it frees the maps and just stores the
 * highest ackId ever acked so that it can always provide a meaningful value
 * for {@link #getAckLevel()}
 * <p>
 * Not all messages are always in {@link #redeliveryScheduleMap}. A message that
 * has been taken using {@link #takeFirstForDelivery(long)} or
 * {@link #takeForReschedule(AckIdV3)} will not be in
 * {@link #redeliveryScheduleMap} but will be in {@link #ackIdMap}.
 * <p>
 * The ack level is managed off of {@link #ackIdMap} so even if a message is not
 * in {@link #redeliveryScheduleMap} the ack level still cannot advance until
 * the message is acknowledged.
 * <p>
 * This class is not thread safe and depends on {@link InflightImpl} to manage
 * access to it so that only one thread uses any instance of this class at a
 * time.
 * 
 * @author stevenso
 */
@NotThreadSafe
public class ReaderInflightData<InfoType> {
    private static final Log log = LogFactory.getLog(ReaderInflightData.class);
    
    private static final String CHECKPOINT_SIGNATURE = "SeqstoreV3::1.0";
    
    public static final <InfoType> Checkpoint<AckIdV3, AckIdV3, InfoType> getEmptyCheckpoint() {
        return new Checkpoint<AckIdV3, AckIdV3, InfoType>(
                CHECKPOINT_SIGNATURE, 
                AckIdV3.MINIMUM,
                AckIdV3.MINIMUM, 
                Collections.<CheckpointEntryInfo<AckIdV3, InfoType>>emptyList());
    }

    @Data
    public static class RedeliverySchedule implements Comparable<RedeliverySchedule> {

        private final long redeliveryTime;

        private final AckIdV3 ackId;

        /**
         * Create a redelivery schedule that is before all messages scheduled
         * for startTime
         * 
         * @param startTime
         */
        public RedeliverySchedule(long startTime) {
            redeliveryTime = startTime;
            ackId = AckIdV3.MINIMUM;
        }

        /**
         * Create a redelivery using the ackid and time from messageInfo
         * 
         * @param messageInfo
         */
        public RedeliverySchedule(AckIdV3 ackId, long scheduledTime) {
            this.redeliveryTime = scheduledTime;
            this.ackId = ackId;
        }

        @Override
        public int compareTo(RedeliverySchedule o) {
            if (this == o)
                return 0;

            if (redeliveryTime < o.redeliveryTime)
                return -1;
            else if (redeliveryTime > o.redeliveryTime)
                return 1;

            return ackId.compareTo(o.ackId);
        }
    }

    /**
     * A map from ack id to the InflightMessageInfoInternal for that ack id.
     */
    private final NavigableMap<AckIdV3, Pair<RedeliverySchedule, InfoType>> ackIdMap;

    /**
     * A map from a RedeliverySchedule to the InflightMessageInfoInternal for the
     * message to redeliver at that time. This only contains messages currently
     * scheduled for redelivery and won't have message that have been take for
     * rescheduling or redelivery.
     */
    private final NavigableMap<RedeliverySchedule, InfoType> redeliveryScheduleMap;

    /**
     * Since messages go into inflight in order then if all messages inflight
     * have been acked the highest AckId ever seen is the ack level
     */
    private AckIdV3 greatestAckIdSeen;

    public ReaderInflightData() {
        ackIdMap = new TreeMap<AckIdV3, Pair<RedeliverySchedule, InfoType>>();
        redeliveryScheduleMap = new TreeMap<RedeliverySchedule, InfoType>();
        greatestAckIdSeen = AckIdV3.MINIMUM;
    }
    
    public ReaderInflightData(Checkpoint<AckIdV3, AckIdV3, InfoType> checkpoint) {
        this();
        
        if( !checkpoint.getSignature().equals( CHECKPOINT_SIGNATURE ) ) {
            log.warn("Ignoring unsupported checkpoint with signature " + checkpoint.getSignature() );
            return;
        }
        
        setAckLevel( checkpoint.getAckLevel() );
        
        for( CheckpointEntryInfo<AckIdV3, InfoType> entry : checkpoint.getInflightMessages()) {
            add( entry.getAckId(), entry.getNextRedriveTime(), entry.getInflightInfo() );
        }
        
        greatestAckIdSeen = checkpoint.getReadLevel();
    }

    /**
     * Add a message to in-flight.
     * 
     * @param ackId ackId used to load the message payload from the store.
     * @param nextRedeliveryTime absolute time (in milliseconds since the epoch)
     *        when the message should become available for re-delivery if not
     *        acked or updated first.
     * @param inflightInfo in-flight message info to associate with the message.
     * @throws IllegalArgumentException if ackId or inflightInfo is <code>null</code>.
     * @throws IllegalStateException if the message is already in flight.
     */
    public void add(AckIdV3 ackId, long nextRedeliveryTime, InfoType inflightInfo) {
        if (ackId == null)
            throw new IllegalArgumentException("ackId is null!");
        
        if( ackId.compareTo( greatestAckIdSeen ) < 0 ) {
            // This can happen under normal circumstances when the inflight is merged with a checkpoint
            log.warn( "Out of order add to inflight of " + ackId + " when " + greatestAckIdSeen + " was already added.");
        } else {
            greatestAckIdSeen = ackId;
        }

        Pair<RedeliverySchedule, InfoType> oldInflight = ackIdMap.get(ackId);
        if (oldInflight != null) throw new IllegalStateException( ackId + " is already in flight." );

        RedeliverySchedule schedule = new RedeliverySchedule(
                ackId, nextRedeliveryTime);
        ackIdMap.put(ackId, new Pair<RedeliverySchedule, InfoType>(schedule, inflightInfo));
        redeliveryScheduleMap.put(schedule, inflightInfo);
    }

    /**
     * Perform the update requested in updateRequest on the message identified
     * by ackId. If successful this returns {@link InflightUpdateResult#DONE DONE}. 
     * If the update cannot be performed because the expectations aren't met 
     * it returns {@link InflightUpdateResult#DONE DONE}. If the message can't
     * be found it returns  {@link InflightUpdateResult#NOT_FOUND NOT_FOUND}.
     * 
     * @return true if the update was done, false if the message wasn't in flight.
     */
    public InflightUpdateResult update(
        AckIdV3 ackId, long currentTime, 
        InflightUpdateRequest<InfoType> updateRequest) 
    {
        Pair<RedeliverySchedule, InfoType> oldEntry = ackIdMap.get(ackId);
        if (oldEntry == null) {
            return InflightUpdateResult.NOT_FOUND;
        }
        
        if ( updateRequest.isExpectedInflightInfoSet() ) {
            InfoType expect = updateRequest.getExpectedInflightInfo();
            if (expect != oldEntry.getValue() && 
                ( expect == null || !expect.equals( oldEntry.getValue() ) ) )
            {
                return InflightUpdateResult.EXPECTATION_UNMET;
            }
        }

        RedeliverySchedule newSchedule = null;
        if (updateRequest.isNewTimeoutSet() ) {
            newSchedule = new RedeliverySchedule(
                    ackId, 
                    MathUtil.addNoOverflow(currentTime, updateRequest.getNewTimeout()));
        }

        if (updateRequest.isNewInflightInfoSet() && newSchedule != null) {
            // rescheduled w/ info change
            ackIdMap.put(ackId,
                    new Pair<RedeliverySchedule, InfoType>(
                            newSchedule, updateRequest.getNewInflightInfo()));
            redeliveryScheduleMap.remove(oldEntry.getKey());
            redeliveryScheduleMap.put(newSchedule, updateRequest.getNewInflightInfo());
        } else if (updateRequest.isNewInflightInfoSet()) {
            // info change only
            ackIdMap.put(ackId,
                    new Pair<RedeliverySchedule, InfoType>(
                            oldEntry.getKey(), updateRequest.getNewInflightInfo()));
            redeliveryScheduleMap.put(oldEntry.getKey(), updateRequest.getNewInflightInfo());
        } else if (newSchedule != null) {
            // reschedule only
            ackIdMap.put(ackId,
                    new Pair<RedeliverySchedule, InfoType>(
                            newSchedule, oldEntry.getValue()));
            redeliveryScheduleMap.remove(oldEntry.getKey());
            redeliveryScheduleMap.put(newSchedule, oldEntry.getValue());
        }

        return InflightUpdateResult.DONE;
    }

    public boolean isInflight(AckIdV3 ackId) {
        return ackIdMap.containsKey(ackId);
    }

    public Pair<RedeliverySchedule, InfoType> getInflightEntry(AckIdV3 ackId) {
        return ackIdMap.get(ackId);
    }

    /**
     * Peek at the first message scheduled for re-delivery before maxTime.
     * 
     * @param maxTime the maximum re-delivery time allowed.
     * @return the first message scheduled for re-delivery before maxTime.
     */
    @CheckForNull
    public Pair<RedeliverySchedule, InfoType> peekFirstScheduled(long maxTime) {
        Map.Entry<RedeliverySchedule, InfoType> entry =
            redeliveryScheduleMap.firstEntry();

        if (entry == null || entry.getKey().getRedeliveryTime() > maxTime) {
            return null;
        } else {
            return new Pair<RedeliverySchedule, InfoType>(entry.getKey(), entry.getValue());
        }        
    }

    public Pair<RedeliverySchedule, InfoType> takeFirstScheduled(long maxTime) {
        Map.Entry<RedeliverySchedule, InfoType> entry =
            redeliveryScheduleMap.firstEntry();

        if (entry == null || entry.getKey().getRedeliveryTime() > maxTime) {
            return null;
        } else {
            redeliveryScheduleMap.remove(entry.getKey());
            return new Pair<RedeliverySchedule, InfoType>(entry.getKey(), entry.getValue());
        }
    }

    public long getTimeOfNextTimeout() {
        Map.Entry<RedeliverySchedule, InfoType> entry =
            redeliveryScheduleMap.firstEntry();

        if( entry == null ) return Long.MAX_VALUE;

        return entry.getKey().getRedeliveryTime();
    }

    public boolean ack(AckIdV3 ackId) {
        if (ackId == null)
            throw new IllegalArgumentException("ackId is null!");

        Pair<RedeliverySchedule, InfoType> messageInfo = ackIdMap.remove(ackId);
        if (messageInfo != null) {
            redeliveryScheduleMap.remove(messageInfo.getKey());
        }

        return messageInfo != null;
    }
    
    public boolean isEmpty() {
        return ackIdMap.isEmpty();
    }

    public int size() {
        return ackIdMap.size();
    }

    @NonNull
    public AckIdV3 getAckLevel() {
        if (isEmpty()) {
            // don't skip a message if isInclusive is false
            if (greatestAckIdSeen.isInclusive() != null ) {
                return greatestAckIdSeen;
            } else {
                return new AckIdV3(greatestAckIdSeen, true);
            }
        }

        return new AckIdV3(ackIdMap.firstKey(), false);
    }

    public void makeAllMessagseDeliverable(long currentTime) {
        if (isEmpty())
            return;

        NavigableMap<RedeliverySchedule, InfoType> tailMap = redeliveryScheduleMap.tailMap(
                new RedeliverySchedule(currentTime + 1), true);

        List<Pair<RedeliverySchedule, InfoType>> newEntries =
            new ArrayList<Pair<RedeliverySchedule, InfoType>>();

        for (Map.Entry<RedeliverySchedule, InfoType> entry : tailMap.entrySet()) {
            RedeliverySchedule schedule = new RedeliverySchedule(
                    entry.getKey().getAckId(), currentTime);
            newEntries.add(new Pair<RedeliverySchedule, InfoType>(schedule, entry.getValue()));
        }

        tailMap.clear();

        for (Pair<RedeliverySchedule, InfoType> newEntry : newEntries) {
            redeliveryScheduleMap.put(newEntry.getKey(), newEntry.getValue());
            ackIdMap.put(newEntry.getKey().getAckId(), newEntry );
        }
    }

    public long setAckLevel(AckIdV3 ackLevel) {
        long messagesRemoved = 0;

        if (!isEmpty()) {
            Iterator<Map.Entry<AckIdV3, Pair<RedeliverySchedule, InfoType>>> itr = ackIdMap.entrySet().iterator();
            while (itr.hasNext()) {
                Map.Entry<AckIdV3, Pair<RedeliverySchedule, InfoType>> entry = itr.next();
                if (entry.getKey().compareTo(ackLevel) > 0)
                    break;

                itr.remove();
                redeliveryScheduleMap.remove(entry.getValue().getKey() );
                messagesRemoved++;
            }
        }

        greatestAckIdSeen = AckIdV3.max(greatestAckIdSeen, ackLevel);

        return messagesRemoved;
    }

    /**
     * Get the inflight metrics. Runs in O( max( log n, #available for delivery
     * ) ).
     * 
     * @return Get the inflight metrics.
     */
    public InflightMetrics getMetrics(long currentTime) {
        if (isEmpty())
            return InflightMetrics.ZeroInFlight;

        int availableForDelivery = 0;
        for (RedeliverySchedule schedule : redeliveryScheduleMap.keySet()) {
            if (schedule.getRedeliveryTime() > currentTime)
                break;
            availableForDelivery++;
        }

        return new InflightMetrics(ackIdMap.size() - availableForDelivery, availableForDelivery);
    }

    /**
     * Get a list of information about all messages currently in flight.
     * 
     * @return
     *     A list of all messages currently in flight.
     */
    public List<Pair<RedeliverySchedule, InfoType>> getAllEntries() {
        if (isEmpty())
            return Collections.emptyList();

        List<Pair<RedeliverySchedule, InfoType>> result =
            new ArrayList<Pair<RedeliverySchedule, InfoType>>(ackIdMap.size());
        result.addAll(ackIdMap.values());
        return result;
    }
    
    /**
     * Returns an unmodifiable sorted set (by id) of the ids of the inflight messages
     * @return
     */
    public NavigableSet<AckIdV3> getAllInflightIds() {
        AckIdV3[] ackIds = ackIdMap.keySet().toArray( new AckIdV3[ ackIdMap.size() ] );
        return new BinarySearchSet<AckIdV3>( Arrays.asList( ackIds ));
    }

    /**
     * Merge the provided set of messages with the inflight state. The merge is done atomically
     * as follows:<br>
     * <br>
     * 1) All inflight messages below the passed in ack level are acked.<br>
     * 2) All inflight messages between the passed in ack level and the checkpoint read level that 
     *    are not in the provided set of messages are acked.<br>
     * 3) All messages in the provided message set that are above the initial read level that are not 
     *    already inflight are added and marked as immediately available for delivery.<br>
     *    
     * @param checkpointAckLevel the ack level to merge with
     * @param checkpointReadLevel the read level from the checkpoint
     * @param reader reader
     * @param messages A set of AckIds of inflight messages to be merged with own in flight message info.
     * 
     * @return boolean true if there are message that the reader knows are acked that the checkpoint
     *   has inflight.
     */
    public boolean mergePartialCheckpoint(Set<AckIdV3> checkPointMessages, AckIdV3 checkpointAckLevel, 
                                          AckIdV3 checkpointReadLevel, long currentTime) 
    {
        boolean providedInflightHasAckedMsg = false;
        AckIdV3 initialAckLevel = getAckLevel();
        AckIdV3 initialReadLevel = getReadLevel();
        
        int ackLevelComparison = checkpointAckLevel.compareTo( initialAckLevel );
        // Step 1 advance the ack level if required, dropping all messages below it.
        if( ackLevelComparison > 0 ) setAckLevel(checkpointAckLevel);
        else if( ackLevelComparison < 0 ) providedInflightHasAckedMsg = true;
        
        // Step 2 - drop all messages between ack level and new read level not in the provided message set
        Iterator<Map.Entry<AckIdV3, Pair<RedeliverySchedule, InfoType>>> itr = ackIdMap.entrySet().iterator();
        while(itr.hasNext()){
            Map.Entry<AckIdV3, Pair<RedeliverySchedule, InfoType>> myEntry = itr.next();
            AckIdV3 ackId = myEntry.getKey();

            // We can't remove entries newer than the read level provided by the checkpoint
            if(ackId.compareTo(checkpointReadLevel) > 0)
                break;

            // Ack anything after the ack level but before the new read level that isn't in checkPointMessages
            if(!checkPointMessages.contains(ackId) ){
                //Remove the message if peer says it is acked.
                redeliveryScheduleMap.remove(myEntry.getValue().getKey());
                itr.remove();
            }
        }
        
        // Step 3 add all messages after the initial read level that are not already inflight
        for(AckIdV3 ackId : checkPointMessages ) {
            if(ackId.compareTo(initialReadLevel) > 0){
                assert !ackIdMap.containsKey(ackId);
                
                Pair<RedeliverySchedule, InfoType> pair =
                    new Pair<RedeliverySchedule, InfoType>(new RedeliverySchedule(ackId, currentTime), null);
                ackIdMap.put(ackId, pair);
                redeliveryScheduleMap.put(pair.getKey(), pair.getValue());
            } else if(!ackIdMap.containsKey(ackId)) {
                providedInflightHasAckedMsg = true;
            }
        }
        
        greatestAckIdSeen = AckIdV3.max( greatestAckIdSeen, checkpointReadLevel );
        
        return providedInflightHasAckedMsg;
    }
    
    /**
     * Get the read level. The read level is defined as being just after the highest message
     * ever inflight.
     * 
     * @return the corrected read level
     */
    public AckIdV3 getReadLevel() {
        if( greatestAckIdSeen.isInclusive() == null ) return new AckIdV3( greatestAckIdSeen, true );
        else return greatestAckIdSeen;
    }

    public Checkpoint<AckIdV3, AckIdV3, InfoType> getCheckpoint(int maximumInflightMessages) {
        AckIdV3 ackLevel = getAckLevel();
        AckIdV3 readLevel = getReadLevel();
        
        if(isEmpty()) {
            return new Checkpoint<AckIdV3, AckIdV3, InfoType>( 
                    CHECKPOINT_SIGNATURE,
                    ackLevel, readLevel, 
                    Collections.<CheckpointEntryInfo<AckIdV3, InfoType>>emptyList() );
        }
        
        if( maximumInflightMessages < 0 ) maximumInflightMessages = ackIdMap.size();
        
        List<CheckpointEntryInfo<AckIdV3, InfoType>> inflightMessages 
            = new ArrayList<CheckpointEntryInfo<AckIdV3,InfoType>>();
        
        AckIdV3 messageAfterLast = null;
        for( Map.Entry<AckIdV3, Pair<RedeliverySchedule, InfoType>> entry : ackIdMap.entrySet() ) {
            if( inflightMessages.size() == maximumInflightMessages ) {
                messageAfterLast = entry.getKey();
                break;
            }
            
            Pair<RedeliverySchedule, InfoType> value = entry.getValue();
            
            inflightMessages.add( new CheckpointEntryInfo<AckIdV3, InfoType>(
                    entry.getKey(), value.getValue(), 
                    value.getKey().getRedeliveryTime()  ) );
        }
        
        if( messageAfterLast != null ) {
            readLevel = new AckIdV3( messageAfterLast, false );
        }
        
        return new Checkpoint<AckIdV3, AckIdV3, InfoType>(
                CHECKPOINT_SIGNATURE, ackLevel, readLevel, inflightMessages );
    }
}
