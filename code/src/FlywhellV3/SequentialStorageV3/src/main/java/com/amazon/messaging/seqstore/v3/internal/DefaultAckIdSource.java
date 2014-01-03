package com.amazon.messaging.seqstore.v3.internal;

import java.util.Iterator;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.messaging.interfaces.Clock;

/**
 * The default implementation of AckIdSource. This implementation returns 
 * assigns AckIdV3s with a minimum time of the current time as returned 
 * by the provided clock.
 * 
 * @author stevenso
 *
 */
public class DefaultAckIdSource implements AckIdSource {
    private final Log log = LogFactory.getLog(DefaultAckIdSource.class);
    
    private final NavigableSet<AckIdV3> enqueuesInProgress = 
        new ConcurrentSkipListSet<AckIdV3>();
    
    
    private final AckIdGenerator ackIdGenerator;
    
    private final Clock clock;
    
    public DefaultAckIdSource(AckIdGenerator ackIdGenerator, Clock clock) {
        this.ackIdGenerator = ackIdGenerator;
        this.clock = clock;
    }

    private long getMinimumAllowedTime() {
        return clock.getCurrentTime();
    }

    // synchronized as getting the ackId and adding it to enqueuesInProgress must be atomic wrt to
    //  getMinimumUncompletedEnqueue.
    @Override
    public final synchronized AckIdV3 requestNewId(long requestedTime) {
        AckIdV3 ackId = ackIdGenerator.getAckId( Math.max( requestedTime, getMinimumAllowedTime()) );
        enqueuesInProgress.add( ackId );
        return ackId;
    }
    
    @Override
    public boolean recordNewId(AckIdV3 ackId) {
        return enqueuesInProgress.contains( ackId );
    }
    
    @Override
    public void enqueueFinished(AckIdV3 ackId) {
        if( !enqueuesInProgress.remove( ackId ) ) {
            log.warn("enqueueFinished with unrecognized ackId" );
        }
    }
    
    @Override
    public synchronized AckIdV3 getMinimumUnfinishedEnqueue() {
        // There is no other way to get the first element that won't throw if there is no first element
        Iterator<AckIdV3> itr = enqueuesInProgress.iterator();
        if( itr.hasNext() ) return itr.next();
        else return null;
    }
    
    @Override
    public boolean isUnfinished(AckIdV3 ackId) {
        return enqueuesInProgress.contains( ackId );
    }
    
    @Override
    public AckIdV3 getMinEnqueueLevel() {
        long minTime = getMinimumAllowedTime();
        AckIdV3 minInProgress = getMinimumUnfinishedEnqueue();
        if( minInProgress != null && minInProgress.getTime() < minTime ) {
            return new AckIdV3( minInProgress, false );
        } else {
            AckIdV3 minNew = ackIdGenerator.getAckId(minTime);
            if( minInProgress != null ) {
                return AckIdV3.min( minNew, new AckIdV3( minInProgress, false ) );
            } else {
                return minNew;
            }
        }
    }
    
    @Override
    public boolean hasUnfinishedEnqueue() {
        return !enqueuesInProgress.isEmpty();
    }
}
