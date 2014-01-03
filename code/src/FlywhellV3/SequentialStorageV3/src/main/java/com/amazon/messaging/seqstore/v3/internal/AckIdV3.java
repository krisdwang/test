package com.amazon.messaging.seqstore.v3.internal;

import net.jcip.annotations.Immutable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import com.amazon.messaging.annotations.TestOnly;
import com.amazon.messaging.seqstore.v3.AckId;

@Immutable
@EqualsAndHashCode(callSuper=false, exclude="bucketPosition")
@ToString(includeFieldNames=false)
public class AckIdV3 implements AckId, Comparable<AckIdV3>  {
    /**
     * The base size in bytes of serialized AckIdV3. Having a non null inclusive flag or 
     * bucket position increases its size. To get the actual size of a specific AckIdV3 use
     * {@link #getSerializedLength()}.
     */
    public static final int LENGTH = 2 * 8 + 4; // 2 longs + 1 integer
    
    public static final AckIdV3 MINIMUM = new AckIdV3( 0, false );
    
    public static final AckIdV3 MAXIMUM = new AckIdV3( Long.MAX_VALUE, true );

    private final long time;

    private final int restartTime;

    private final long seq;
    
    /**
     * The position of this AckIdV3 in the bucket if known. No matter what
     * the value of isInclusive this is the position of the core AckIdV3 not
     * the one immediately after or before it.
     */
    private final Long bucketPosition;

    /**
     * Used to indicate if this level is inclusive of the ackId it represents or
     * not.
     */
    private final Boolean isInclusive;
    
    public AckIdV3(byte[] bytes) {
        this(bytes, 0, bytes.length);
    }
    
    public AckIdV3(byte[] bytes, int offset, int length) {
        int maxPosition = length + offset;
        time = bytesToLong( bytes, offset );
        offset += 8;
        
        if( time < 0 ) throw new IllegalArgumentException("AckIdV3 time cannot be negative.");
        
        restartTime = bytesToInt(bytes, offset);
        offset += 4;
        
        seq = bytesToLong(bytes, offset);
        offset += 8;
        
        if( maxPosition >= offset + 8 ) {
            bucketPosition = bytesToLong(bytes, offset);
            if( bucketPosition.longValue() < 0 ) throw new IllegalArgumentException("Invalid bucket position" );
            offset += 8;
        } else {
            bucketPosition = null;
        }
        
        if( maxPosition > offset) {
            switch( bytes[offset] ) {
            case 0:
                isInclusive = false;
                break;
            case 1:
                isInclusive = true;
                break;
            default:    
                throw new IllegalArgumentException( "Invalid byte at " + offset );
            }
            offset += 1;
        } else {
            isInclusive = null;
        }
        
        if( maxPosition != offset ) {
            throw new IllegalArgumentException("Bytes left over after converting AckIdV3");
        }
    }
    
    public AckIdV3(AckIdV3 old, long bucketPosition) {
        this.time = old.time;
        this.seq = old.seq;
        this.restartTime = old.restartTime;
        this.isInclusive = old.isInclusive;
        this.bucketPosition = bucketPosition;
    }

    AckIdV3(long time, long seq, int restartCount) {
        if( time < 0 ) throw new IllegalArgumentException("AckIdV3 time cannot be negative.");
        this.time = time;
        this.restartTime = restartCount;
        this.seq = seq;
        this.bucketPosition = null;
        this.isInclusive = null;
    }
    
    public AckIdV3(long time, boolean isInclusive) {
        if( time < 0 ) throw new IllegalArgumentException("AckIdV3 time cannot be negative.");
        this.time = time;
        this.bucketPosition = null;
        if (isInclusive) {
            this.seq = Long.MAX_VALUE;
            this.isInclusive = true;
            this.restartTime = Integer.MAX_VALUE;
        } else {
            this.seq = 0;
            this.isInclusive = false;
            this.restartTime = 0;
        }
    }

    public AckIdV3(AckIdV3 old, boolean isInclusive) {
        this.time = old.time;
        this.seq = old.seq;
        this.restartTime = old.restartTime;
        this.bucketPosition = old.bucketPosition;
        this.isInclusive = isInclusive;
    }
    
    @TestOnly
    public AckIdV3(AckIdV3 old, Boolean isInclusive, Long bucketPosition) {
        this.time = old.time;
        this.seq = old.seq;
        this.restartTime = old.restartTime;
        this.bucketPosition = bucketPosition;
        this.isInclusive = isInclusive;
    }
    
    @TestOnly
    AckIdV3(long time, long seq, int restartCount, Boolean isInclusive, Long bucketPosition) {
        if( time < 0 ) throw new IllegalArgumentException("AckIdV3 time cannot be negative.");
        this.time = time;
        this.seq = seq;
        this.isInclusive = isInclusive;
        this.restartTime = restartCount;
        this.bucketPosition = bucketPosition;
    }
    
    private static long bytesToLong( final byte[]bytes, final int offset) {
        long retval = 0;
        for (int i = 0; i < 8; i++) {
            retval = (retval << 8) + (bytes[i+offset] & 0xff);
        }
        return retval;
    }
    
    private static int bytesToInt( final byte[]bytes, final int offset) {
        int retval = 0;
        for (int i = 0; i < 4; i++) {
            retval = (retval << 8) + (bytes[i+offset] & 0xff);
        }
        return retval;
    }
    
    private static void bytesFromLong( final long value, final byte[]bytes, final int offset) {
        for (int i = 0; i < 8; i++) {
            bytes[i + offset] = (byte) (value >> (56 - i * 8));
        }
    }
    
    private static void bytesFromInt( final int value, final byte[]bytes, final int offset) {
        for (int i = 0; i < 4; i++) {
            bytes[i + offset] = (byte) (value >> (24 - i * 8));
        }
    }

    public long getTime() {
        return time;
    }

    public long getSeq() {
        return seq;
    }

    public Boolean isInclusive() {
        return isInclusive;
    }

    public int getRestartTime() {
        return restartTime;
    }
    
    
    public Long getBucketPosition() {
        return bucketPosition;
    }
    
    @Override
    public byte[] toBytes() {
        int requiredSize = getSerializedLength();
        byte[] result = new byte[requiredSize];
        writeToArrayImpl(result, 0);
        return result;
    }
    
    public int writeToArray(byte[] array, int offset) {
        int requiredSize = getSerializedLength();
        if( offset + requiredSize > array.length ) {
            throw new IllegalArgumentException( 
                    "array is not large enough, Need " + requiredSize + " bytes but only had" +
            		( array.length - offset ) + " bytes left" ); 
        }
        
        writeToArrayImpl( array, offset );
        return requiredSize;
    }
    
    private void writeToArrayImpl(byte array[], int offset) {
        bytesFromLong(time,array,offset);
        offset += 8;
        bytesFromInt(restartTime,array, offset);
        offset += 4;
        bytesFromLong(seq,array,offset);
        offset += 8;
        if( bucketPosition != null ) {
            bytesFromLong(bucketPosition,array,offset);
            offset += 8;
        }
        if (isInclusive != null) {
            array[offset] = (byte) (isInclusive ? 1 : 0);
            offset++;
        }
    }

    /**
     * Get the length of the byte array that should be used to serialize this AckIdV3
     * @return
     */
    public int getSerializedLength() {
        int requiredSize = LENGTH;
        if(isInclusive != null ) requiredSize++; // An extra byte for the isInclusive flag.
        if( bucketPosition != null ) requiredSize += 8; // An extra long for the position
        return requiredSize;
    }
    
    public AckIdV3 getCoreAckId() {
        if( isInclusive != null || bucketPosition != null ) {
            return new AckIdV3( time, seq, restartTime );
        }
        
        return this;
    }

    /**
     * Return an AckIdV3 with the bucketPosition if any dropped. This AckIdV3 should
     * be used for exchanging AckIdV3s with other SeqStore instances where the bucket
     * start/stop positions may not be the same.
     * 
     * @return
     */
    public AckIdV3 getBucketIndependentAckId() {
        if( bucketPosition != null ) {
            return new AckIdV3( time, seq, restartTime, isInclusive, null );
        }
        
        return this;
    }
    
    @Override
    public int compareTo(AckIdV3 o) {
        if (time > o.time) {
            return 1;
        } else if (time < o.time) {
            return -1;
        } else {
            if (restartTime > o.restartTime) {
                return 1;
            } else if (restartTime < o.restartTime) {
                return -1;
            } else {
                if (seq > o.seq) {
                    return 1;
                } else if (seq < o.seq) {
                    return -1;
                } else {
                    if (isInclusive == null) {
                        if (o.isInclusive == null)
                            return 0;
                        if (o.isInclusive)
                            return -1;
                        else
                            return 1;
                    } else if (isInclusive) {
                        if ((o.isInclusive == null) || !o.isInclusive)
                            return 1;
                        else
                            return 0;
                    } else if (!isInclusive) {
                        if ((o.isInclusive == null) || o.isInclusive)
                            return -1;
                        else
                            return 0;
                    } else {
                        return 0;
                    }
                }
            }
        }
    }

    public static AckIdV3 min(AckIdV3 a, AckIdV3 b) {
        if (a == null)
            return b;
        if (b == null)
            return a;
        if (a.compareTo(b) <= 0)
            return a;
        return b;
    }

    public static AckIdV3 max(AckIdV3 a, AckIdV3 b) {
        if (a == null)
            return b;
        if (b == null)
            return a;
        if (a.compareTo(b) >= 0)
            return a;
        return b;
    }

}
