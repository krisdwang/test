package com.amazon.cluster;

public class JoinFailureException extends Exception {

    public JoinFailureException(Throwable cause) {
	super(cause);
    }

    public JoinFailureException(String message, Throwable cause) {
	super(message, cause);
    }

}