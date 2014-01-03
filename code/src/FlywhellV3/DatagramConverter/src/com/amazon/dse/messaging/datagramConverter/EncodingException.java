
package com.amazon.dse.messaging.datagramConverter;

/**
 * An exception to be trown if there is a problem encoding a message. 
 */
public class EncodingException extends Exception {
	
	/**
	 * An unknown problem occured while encoding.
	 */
	public EncodingException() {
		this("");
	}
	
	/**
	 * @param message The problem that occured.
	 * @param cause The cause of the problem.
	 */
	public EncodingException(String message, Throwable cause) {
		this(message);
		this.initCause(cause);
	}
	/**
	 * @param message The problem that occured.
	 */
	public EncodingException(String message) {
		super(message);
	}
}