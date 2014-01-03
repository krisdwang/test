package com.amazon.dse.messaging.datagramConverter;

/**
 * An exception to be trown if there is a problem decoding a message. 
 */
public class DecodingException extends Exception {

	/**
	 * An unknown problem occured while decoding.
	 */
	public DecodingException() {
		this("");
	}

	/**
	 * @param message The problem that occured.
	 * @param cause The cause of the problem.
	 */
	public DecodingException(String message, Throwable cause) {
		this(message);
		this.initCause(cause);
	}

	/**
	 * @param message The problem that occured.
	 */
	public DecodingException(String message) {
		super(message);
	}
}