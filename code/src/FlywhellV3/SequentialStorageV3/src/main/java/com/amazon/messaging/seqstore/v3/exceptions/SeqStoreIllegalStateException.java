package com.amazon.messaging.seqstore.v3.exceptions;

/**
 * Used to report that operation cannot be called in current SeqStore state
 * 
 * @author fateev
 */
public abstract class SeqStoreIllegalStateException extends SeqStoreException {

    private static final long serialVersionUID = 6229835378218733327L;

    public SeqStoreIllegalStateException(String msg, Throwable ex) {
        super(msg, ex);
    }

    public SeqStoreIllegalStateException(String msg) {
        super(msg);
    }

    public SeqStoreIllegalStateException(Throwable ex) {
        super(ex);
    }

}
