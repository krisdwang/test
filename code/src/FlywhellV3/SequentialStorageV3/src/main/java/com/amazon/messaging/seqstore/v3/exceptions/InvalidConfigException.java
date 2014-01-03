package com.amazon.messaging.seqstore.v3.exceptions;

public class InvalidConfigException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public InvalidConfigException(String message) {
        super(message);
    }

    public InvalidConfigException(String message, Throwable cause) {
        super(message, cause);
    }
}