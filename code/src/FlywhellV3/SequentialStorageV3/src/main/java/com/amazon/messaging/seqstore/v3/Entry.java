package com.amazon.messaging.seqstore.v3;

import java.util.Arrays;

/**
 * Data kept in a storage.
 */
public abstract class Entry {

    /**
     * LogId is used to identify message in log statements related to its
     * processing inside SeqStore.
     */
    public String getLogId() {
        return null;
    }

    /**
     * Application specific content.
     * 
     * @return not null
     */
    public abstract byte[] getPayload();
    
    /**
     * Get the size of the payload. The default implementation returns getPayload().length. 
     * 
     * @return
     */
    public int getPayloadSize() {
        return getPayload().length;
    }

    /**
     * @return The time this message is available for dequeue. For non-timer
     *         queues this returns the enqueue time.
     */
    public long getAvailableTime() {
        return 0;
    }

    /**
     * A basic equals for Entry that checks that the log id and payload and available times are the same
     */
    @Override
    public boolean equals(Object obj) {
        if( !( obj instanceof Entry ) ) return false;
        
        Entry other = ( Entry ) obj;
        if( other.getLogId() != getLogId() ) {
            if( other.getLogId() == null ) return false;
            if( !other.getLogId().equals( getLogId() ) ) return false;
        }
        
        if( other.getAvailableTime() != getAvailableTime() ) return false;
        
        if( !Arrays.equals( other.getPayload(), getPayload() ) ) return false;
        
        return true;
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        
        String logId = getLogId();
        if( logId != null ) hashCode = logId.hashCode();
        
        byte[] payload = getPayload();
        if( payload != null ) hashCode = 7 * hashCode + Arrays.hashCode( payload );
        
        long availableTime = getAvailableTime();
        hashCode = 7 * hashCode + ( int ) ( availableTime ^ availableTime >>> 32 ); 
        
        return hashCode;
    }
    
    @Override
    public String toString() {
        return 
            "{logId=" + getLogId() + ",availableTime=" + getAvailableTime() + 
            ",payload=[" + getPayload().length + " bytes]"  + "}";
    }
}
