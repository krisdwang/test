/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.dse.messaging.datagram;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import com.amazon.dse.messaging.datagramConverter.BufferDatagramConverter;
import com.amazon.dse.messaging.datagramConverter.DecodingException;
import com.amazon.dse.messaging.datagramConverter.EncodingException;

import com.amazon.ion.stateless.IonConstants;
import com.amazon.ion.streaming.IonBinaryWriter;
import com.amazon.ion.streaming.IonIterator;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonWriter;
import com.amazon.ion.streaming.SimpleByteBuffer;
import com.amazon.ion.streaming.SimpleByteBuffer.*;

public class IonDatagramConverter extends BufferDatagramConverter {

    private static final IonTypedCreator ionMaker = new IonTypedCreator();

    @Override
    public byte[] serialize(Map message) throws EncodingException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        try {
            bout.write(new byte[]{0,0,0,0}); //Placeholder for length.
            IonWriter writer = new IonBinaryWriter();
            ionMaker.writeValue(message, writer);
            bout.write(writer.getBytes());
            byte [] result = bout.toByteArray();
            result[0] = (byte) (result.length >> 24);
            result[1] = (byte) (result.length >> 16);
            result[2] = (byte) (result.length >> 8);
            result[3] = (byte) result.length;
            return result;
        } catch (IOException e) {
            throw new EncodingException("Error serializing datagram", e);
        }
    }
    
    /**
     * Ion does lazy deserialization so this is an unnessicary optimization.
     * @see amazon.platform.clienttoolkit.util.DatagramConverter#deserialize(byte[], int, int, int)
     */
    @Override
    public Map deserialize(byte[] buf, int offset, int length, int depth) throws DecodingException {
        return deserialize(buf, offset, length);
    }

    @Override
    public Map deserialize(byte[] buf, int offset, int length) throws DecodingException {
        if (buf.length-offset < 8 || buf.length-offset < length)
            throw new IllegalArgumentException("Buffer to short!");
        
        int clamedSize = byteArrayToInt(buf, offset);
        if (clamedSize > length)
            throw new IllegalArgumentException("Datagram claims to be larger than the buffer.");
        
        if (buf[offset+4] != IonConstants.BINARY_VERSION_MARKER_1_0[0] ||
                buf[offset+5] != IonConstants.BINARY_VERSION_MARKER_1_0[1] ||
                buf[offset+6] != IonConstants.BINARY_VERSION_MARKER_1_0[2] ||
                buf[offset+7] != IonConstants.BINARY_VERSION_MARKER_1_0[3] )
            throw new IllegalArgumentException("Magic bytes are not for an Ion datagram!");
       
        IonReader iter = IonIterator.makeIterator(buf, offset+4, length-4);
        iter.hasNext();
        try {
            return (Map) ionMaker.getOrigionalValue(iter);
        } catch (IOException e) {
            throw new DecodingException("Error deserializing datagram", e);
        }
    }

    @Override
    public String getFormatName() {
        return "Ion";
    }

    @Override
    public int getMagicBytes() {
        return byteArrayToInt(IonConstants.BINARY_VERSION_MARKER_1_0, 0);
    }
    
//    private int byteArrayToInt(byte[] bytes, int offset) {
//        return ((bytes[offset + 0] << 24) & 0xFF000000)
//        + ((bytes[offset + 1] << 16) & 0x00FF0000)
//        + ((bytes[offset + 2] << 8) & 0x0000FF00)
//        + ((bytes[offset + 3]) & 0x000000FF);
//    }
    
    /**
     * Ion does partial deserialization all the time by doing deserialization
     * lazily. Therefore any object retrieved from a datagram is always
     * "deserialized" so this method always returns false.
     * 
     * @see amazon.platform.clienttoolkit.util.DatagramConverter#isSerialized(java.lang.Object)
     */
    @Override
    public boolean isSerialized(Object element) {
        return false;
    }


}
