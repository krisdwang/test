/**
 * 
 */
package com.amazon.dse.discovery;

/**
 * @author Harshawardhan Gadgil <hgadgil@amazon.com>
 * 
 */
public class DeleteException extends Exception {

    /**
     * 
     */
    private static final long serialVersionUID = -5703221692111458944L;

    /**
     * 
     */
    public DeleteException() {
    // TODO Auto-generated constructor stub
    }

    /**
     * @param message
     */
    public DeleteException(String message) {
	super(message);
	// TODO Auto-generated constructor stub
    }

    /**
     * @param cause
     */
    public DeleteException(Throwable cause) {
	super(cause);
	// TODO Auto-generated constructor stub
    }

    /**
     * @param message
     * @param cause
     */
    public DeleteException(String message, Throwable cause) {
	super(message, cause);
	// TODO Auto-generated constructor stub
    }

}
