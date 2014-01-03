/**
 *      Copyright (c) 2004 Amazon.com Inc. All Rights Reserved.
 *      AMAZON.COM CONFIDENTIAL
 */
package com.amazon.dse.messaging.datagramConverter;

import java.util.Map;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

/**
 * DatagramConverter
 * 
 * Convert a datagram stream from a serialized format to a map.
 */
public abstract class DatagramConverter {

	/**
	 * @param buf input representing key / value pairs
	 * @return map representation of the buffer
	 * @throws DecodingException
	 */
	abstract public Map deserialize(byte[] buf) throws DecodingException;

	/**
	 * @param buf input representing key / value pairs
	 * @param depth How deep this message should be deserialized to.
	 * @return map representation of the buffer
	 * @throws DecodingException
	 *             If the message buf is invalid for any reason.
	 */
	abstract public Map deserialize(byte[] buf, int depth)
			throws DecodingException;

	/**
	 * Converts a serialized byte array into a map for use. This version takes
	 * an offset and length so as to copying the bytes if they are embedded in a
	 * larger string. (The string will not be modified in any way.)
	 * 
	 * @param buf
	 *            The message as it came over the wire.
	 * @param offset
	 *            Where the message starts in the byte array.
	 * @param length
	 *            How long the message is.
	 * @return The decoded map that it represents.
	 * @throws DecodingException
	 *             If the message buf is invalid for any reason.
	 */
	abstract public Map deserialize(byte[] buf, int offset, int length)
			throws DecodingException;

	/**
	 * Converts a serialized byte array into a map for use. This version takes
	 * an offset and length so as to copying the bytes if they are embedded in a
	 * larger string. (The string will not be modified in any way.)
	 * 
	 * @param buf
	 *            The message as it came over the wire.
	 * @param offset
	 *            Where the message starts in the byte array.
	 * @param length
	 *            How long the message is.
	 * @param depth How deep this message should be deserialized to.
	 * @return The decoded map that it represents.
	 * @throws DecodingException
	 *             If the message buf is invalid for any reason.
	 */
	abstract public Map deserialize(byte[] buf, int offset, int length, int depth)
			throws DecodingException;
	
	/**
	 * @param is -
	 *            input stream representing key - value pairs, of arbitrary
	 *            complexity
	 * @return map representation of the stream
	 * @throws DecodingException
	 * @throws IOExceptionCan
	 */
	abstract public Map deserialize(InputStream is) throws DecodingException,
			IOException;

	/**
	 * @param is -
	 *            input stream representing key - value pairs, of arbitrary
	 *            copmlexity
	 * @param depth
	 *            Defines depth of conversion. Elements that are on lower levels
	 *            are returned as wrapper objects. To check if . Minimal value
	 *            is 1 as this method returns Map. When value is 1 values of all
	 *            non primite fields of result datagram are kept serialized.
	 * @return map representation of the stream with possibly some subdatagrams
	 *         in serialized format. This partially deserialized message can be
	 *         passed back to serialize which should handle it correctly.
	 * @throws DecodingException
	 * @throws IOException
	 */
	abstract public Map deserialize(InputStream is, int depth)
			throws DecodingException, IOException;

	/**
	 * Serialize a message map into a byte array. Useful in situations where it
	 * is desired to know the size of the output, which would be obscured by
	 * using {@link #serialize(OutputStream, Map)}
	 * 
	 * @param message
	 * @return The serialized form of message
	 * @throws EncodingException
	 *             If the message is invalid for any reason.
	 */
	abstract public byte[] serialize(Map message) throws EncodingException;
	
	/**
	 * @param message
	 *            map representation of the message. Can contain subdatagrams in
	 *            serialized format as instances of {@link SerializedDatagram}
	 */
	abstract public void serialize(OutputStream os, Map message)
			throws EncodingException, IOException;

	/**
	 * Used to check if an element of a message is in serialized form.
	 */
	abstract public boolean isSerialized(Object element);
	
	/**
	 * @return The 4 magic bytes that will identify this format on the wire.
	 */
	abstract public int getMagicBytes();
	
	/**
     * @return The name for this wireformat.
     */
    abstract public String getFormatName();
		
}
