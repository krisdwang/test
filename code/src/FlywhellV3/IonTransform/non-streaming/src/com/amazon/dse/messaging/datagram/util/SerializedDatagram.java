/*
 * SerializedMessageMap.java
 *
 * Copyright (c) 2005 Amazon.com.  All rights reserved.
 * 
 */
package com.amazon.dse.messaging.datagram.util;

import java.io.ByteArrayOutputStream;

import amazon.platform.clienttoolkit.util.EncodingException;

/**
 * Represents subdatagram that is kept in serialized form. Useful when
 * subdatagram is not to be accessed/modified but retreived/stored from/to DB or
 * sent to external consumer.
 * 
 * @author kaitchuc
 */
abstract public interface SerializedDatagram {

	/**
	 * For internal use only. Do NOT depend directly on this function.
	 * 
	 * @throws EncodingException
	 */
	abstract void writeTo(ByteArrayOutputStream bout) throws EncodingException;

	/**
	 * @return The length of this serialized object in bytes.
	 * @throws EncodingException
	 *             If this object cannot be serialized.
	 */
	abstract int getLength() throws EncodingException;
}
