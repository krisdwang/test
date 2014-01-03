package com.amazon.dse.discovery;

/**
 * RegisterException is raised if there is a failure to register the given
 * <code> name </code> with the directory service
 */
public class RegisterException extends Exception {

    private static final long serialVersionUID = -6678905648982994480L;

    public RegisterException() {}

    public RegisterException(String message) {
	super(message);
    }

    public RegisterException(Throwable cause) {
	super(cause);
    }

    public RegisterException(String message, Throwable cause) {
	super(message, cause);
    }

}
