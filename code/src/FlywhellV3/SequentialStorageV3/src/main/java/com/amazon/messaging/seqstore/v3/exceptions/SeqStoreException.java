package com.amazon.messaging.seqstore.v3.exceptions;

public abstract class SeqStoreException extends Exception {

    private static final long serialVersionUID = 3764132372378140196L;

    public SeqStoreException(String msg, Throwable ex) {
        super(msg, ex);
    }

    public SeqStoreException(String msg) {
        super(msg);
    }

    public SeqStoreException(Throwable ex) {
        super(ex);
    }

}
