package com.amazon.messaging.utils;

/**
 * Thrown when a SettableFuture is set twice.
 * 
 * @author stevenso
 *
 */
public class DuplicateSetException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    public DuplicateSetException() {
        super();
    }

    public DuplicateSetException(String message, Throwable cause) {
        super(message, cause);
    }

    public DuplicateSetException(String s) {
        super(s);
    }

    public DuplicateSetException(Throwable cause) {
        super(cause);
    }
}
