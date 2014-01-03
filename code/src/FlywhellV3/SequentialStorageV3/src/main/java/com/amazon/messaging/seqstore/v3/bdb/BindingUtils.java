package com.amazon.messaging.seqstore.v3.bdb;

import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;


public class BindingUtils {
    /**
     * Write a variable length AckIdV3. This AckIdV3 is saved by first writing the length of the id as an integer 
     * and then writing the serialized id.
     */
    public static void writeAckIdV3(TupleOutput tout, AckIdV3 ackId) {
        int length = ackId.getSerializedLength();
        tout.makeSpace( length + 4 );
        tout.writeInt( length );
        ackId.writeToArray( tout.getBufferBytes(), tout.size() );
        tout.addSize( length );
    }
    
    /**
     * Read an AckIdV3 as saved by #writeAckIdV3. 
     */
    public static AckIdV3 readAckIdV3(TupleInput tin) {
        int length = tin.readInt();
        AckIdV3 retval = new AckIdV3( tin.getBufferBytes(), tin.getBufferOffset(), length );
        tin.skipFast( length );
        return retval;
    }
    
    /**
     * Write a raw AckIdV3. Unlike {@link #writeAckIdV3(TupleOutput, AckIdV3)} this does not write the length
     * of the AckIdV3, it just writes its serialized form. This can be used when the AckIdV3 is the last part
     * of a the output and can always be assumed to use the remaining bytes or when the size of the id
     * is known to be constant.
     */
    public static void writeRawAckIdV3(TupleOutput tout, AckIdV3 ackId) {
        int length = ackId.getSerializedLength();
        tout.makeSpace(length);
        ackId.writeToArray( tout.getBufferBytes(), tout.size() );
        tout.addSize(length);
    }
    
    /**
     * Read an AckIdV3 as saved by {@link #writeRawAckIdV3(TupleOutput, AckIdV3)}
     * 
     * @param tin the input to read the AckIdV3 from
     * @param length the length of the AckIdV3. If -1 then the remaining length of tin is used
     */
    public static AckIdV3 readRawAckIdV3(TupleInput tin, int length) {
        if( length == -1 ) {
            length = tin.getBufferLength() - tin.getBufferOffset();
        }
        AckIdV3 retval = new AckIdV3( tin.getBufferBytes(), tin.getBufferOffset(), length );
        tin.skipFast( length );
        return retval;
    }
    
    public static void writeBytes(TupleOutput tout, byte[] toWrite) {
        tout.makeSpace( 4 + toWrite.length );
        tout.writeInt(toWrite.length);
        tout.writeFast(toWrite);
    }

    public static byte[] readBytes(TupleInput tin) {
        int length = tin.readInt();
        byte[] bytes = new byte[length];
        int read = tin.readFast(bytes);
        assert read == length;
        return bytes;
    }
}
