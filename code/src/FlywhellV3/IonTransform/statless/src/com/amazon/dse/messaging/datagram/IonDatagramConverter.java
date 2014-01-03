/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.dse.messaging.datagram;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import com.amazon.dse.messaging.datagramConverter.BufferDatagramConverter;
import com.amazon.dse.messaging.datagramConverter.DecodingException;
import com.amazon.dse.messaging.datagramConverter.EncodingException;
import com.amazon.ion.stateless.IonBinary;
import com.amazon.ion.stateless.IonConstants;


public class IonDatagramConverter extends BufferDatagramConverter {

    private static final IonTypedCreator ionMaker = new IonTypedCreator();

    @Override
    public byte[] serialize(Map message) throws EncodingException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        try {
            bout.write(new byte[]{0,0,0,0}); //Placeholder for length.
            bout.write(IonConstants.BINARY_VERSION_MARKER_1_0);
            ionMaker.writeValue(message, bout, null);
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
     * @see com.amazon.dse.messaging.datagramConverter.DatagramConverter#deserialize(byte[], int, int, int)
     */
    @Override
    public Map deserialize(byte[] buf, int offset, int length, int depth) throws DecodingException {
        return deserialize(buf, offset, length);
    }

    @Override
    public Map deserialize(byte[] in, int offset, int length) throws DecodingException {
        if (in.length-offset < 8 || in.length-offset < length)
            throw new IllegalArgumentException("Buffer to short!");
        
        int clamedSize = byteArrayToInt(in, offset);
        if (clamedSize > length)
            throw new IllegalArgumentException("Datagram claims to be larger than the buffer.");
        
        if (!IonBinary.startsWithBinaryVersionMarker(in,offset+4,length-4))
            throw new IllegalArgumentException("Magic bytes are not for an Ion datagram!");
            
        ByteBuffer buff = ByteBuffer.wrap(in,offset+8,length-8);
        try {
            return (Map) ionMaker.getOrigionalValue(buff, null);
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
    /**
     * Ion does partial deserialization all the time by doing deserialization
     * lazily. Therefore any object retrieved from a datagram is always
     * "deserialized" so this method always returns false.
     * 
     * @see com.amazon.dse.messaging.datagramConverter.DatagramConverter#isSerialized(java.lang.Object)
     */
    @Override
    public boolean isSerialized(Object element) {
        return false;
    }


}
