package com.amazon.messaging.seqstore.v3.exceptions;

/**
 * Used to report problems internal to SeqStore implementation.
 * 
 * @author fateev
 */
public class SeqStoreInternalException extends SeqStoreException {

    private static final long serialVersionUID = -1815517270874088410L;

    public SeqStoreInternalException(String msg, Throwable ex) {
        super(msg, ex);
    }

    public SeqStoreInternalException(String msg) {
        super(msg);
    }

    public SeqStoreInternalException(Throwable ex) {
        super(ex);
    }

}
