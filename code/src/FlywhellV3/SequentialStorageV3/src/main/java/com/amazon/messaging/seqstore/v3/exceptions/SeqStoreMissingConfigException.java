package com.amazon.messaging.seqstore.v3.exceptions;


public class SeqStoreMissingConfigException extends SeqStoreIllegalStateException {
    private static final long serialVersionUID = 1L;

    public SeqStoreMissingConfigException(String msg, Throwable ex) {
        super(msg, ex);
    }

    public SeqStoreMissingConfigException(String msg) {
        super(msg);
    }

    public SeqStoreMissingConfigException(Throwable ex) {
        super(ex);
    }
}
