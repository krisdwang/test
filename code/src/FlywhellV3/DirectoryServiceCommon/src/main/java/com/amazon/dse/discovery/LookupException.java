package com.amazon.dse.discovery;

/**
 * LookupException is thrown when a directory service fails in its lookup for
 * any reason, or if if no records are found associated with the
 * <code> name </code>
 */

public class LookupException extends Exception {

    private static final long serialVersionUID = -5858086153012056436L;

    public LookupException() {
	super();
    }

    public LookupException(String message, Throwable cause) {
	super(message, cause);
    }

    public LookupException(String message) {
	super(message);
    }

    public LookupException(Throwable cause) {
	super(cause);
    }

}
