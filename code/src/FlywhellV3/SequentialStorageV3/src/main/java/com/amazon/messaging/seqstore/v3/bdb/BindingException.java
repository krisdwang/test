package com.amazon.messaging.seqstore.v3.bdb;

import com.sleepycat.je.DatabaseException;


public class BindingException extends DatabaseException {
    private static final long serialVersionUID = 1L;

    public BindingException(String message, Throwable t) {
        super(message, t);
    }

    public BindingException(String message) {
        super(message);
    }

    public BindingException(Throwable t) {
        super(t);
    }
}
