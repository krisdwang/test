/**
 *      Copyright (c) 2004 Amazon.com Inc. All Rights Reserved.
 *      AMAZON.COM CONFIDENTIAL
 */
package com.amazon.dse.messaging.datagramConverter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;



/**
 * BufferDatagramConverter
 * 
 * This is the class that maps all stream operations into underlying byte buffer
 * operations. In effect this is a stream adapter for byte[] operations.
 * 
 */
public abstract class BufferDatagramConverter extends DatagramConverter {
	// Size of the internal buffer used when converting a POST data stream
	// into a byte array
	private static int bufferSize_ = 16384;


	 public Map deserialize(byte[] buf) throws DecodingException {
	     return deserialize(buf, Integer.MAX_VALUE);
	 }
	
	public Map deserialize(byte[] buf, int offset, int length) 
            throws DecodingException {
	     return deserialize(buf, offset, length, Integer.MAX_VALUE);
	 }
	 
	 public Map deserialize(byte[] buf, int depth) 
            throws DecodingException {
	     return deserialize(buf, 0, buf.length, depth);
	 }
	 
	/**
	 * @see com.amazon.dse.messaging.datagramConverter.DatagramConverter#deserialize(java.io.InputStream)
	 */
	public Map deserialize(InputStream is) throws IOException,
			DecodingException {
		return deserialize(is, Integer.MAX_VALUE);
	}

	/**
	 * @see com.amazon.dse.messaging.datagramConverter.DatagramConverter#deserialize(java.io.InputStream, int)
	 */
    public Map deserialize(InputStream is, int depth) throws IOException,
            DecodingException {
        final int SIZE_OF_LENGTH_FIELD = 4;

        byte[] lengthBuf = new byte[SIZE_OF_LENGTH_FIELD];
        is.mark(bufferSize_); // mark the current position
        readFromInputStream(is, SIZE_OF_LENGTH_FIELD, lengthBuf);
        int length = byteArrayToInt(lengthBuf, 0);
        is.reset(); // rewind to the marked position
        byte[] datagramBuf = new byte[length];
        readFromInputStream(is, length, datagramBuf);

        Map result = deserialize(datagramBuf, depth);
        return result;
    }

    /**
     * Read numBytesToRead bytes from istream into buffer
     * 
     * @param istream
     *            InputStream to read from
     * @param numBytesToRead
     *            how many bytes to read from istream
     * @param buffer
     *            where to stored bytes read
     * @throws IOException
     */
    private static void readFromInputStream(InputStream istream,
            final int numBytesToRead, byte[] buffer) throws IOException {
        assert (buffer.length >= numBytesToRead);

        int totalBytesReadSoFar = 0;
        while (totalBytesReadSoFar < numBytesToRead) {
            int bytesRead = istream.read(buffer, totalBytesReadSoFar,
                    numBytesToRead - totalBytesReadSoFar);
            if (bytesRead <= 0)
                throw new IOException("Failed to read " + numBytesToRead
                        + " bytes");

            totalBytesReadSoFar += bytesRead;
        }
    }

	/**
	 * Write a serialized form of a message into an output stream. If the size
	 * of the output is desired, use {@link #serialize(Map)}
	 * 
	 * @param os The outputstream that you want the message writen to.
	 * @param message The message you want to send.
	 * @throws IOException If there is a problem with the outputstream.
	 * @throws {@link EncodingException} If there is a problem serializing the mseeage.
	 * @see com.amazon.dse.messaging.datagramConverter.DatagramConverter#serialize(java.io.OutputStream, java.util.Map)
	 */
	public void serialize(OutputStream os, Map message) throws IOException,
			EncodingException {
		byte[] responseDatagram = serialize(message);
		os.write(responseDatagram);
	}

	/**
	 * Call this if you want to change the buffer size that is used to extract
	 * messages from an input stream.
	 *  (The default is 16384)
	 * @param bufferSize The size you want the buffer to be in bytes.
	 */
	public void setBufferSize(int bufferSize) {
		if (bufferSize < 1)
			throw new IllegalArgumentException("non positive buffer size");
		bufferSize_ = bufferSize;
	}
   
    protected static int byteArrayToInt(byte[] bytes, int offset) {
        return ((bytes[offset + 0] << 24) & 0xFF000000)
                + ((bytes[offset + 1] << 16) & 0x00FF0000)
                + ((bytes[offset + 2] << 8) & 0x0000FF00)
                + ((bytes[offset + 3]) & 0x000000FF);
    }

}
