package com.amazon.messaging.seqstore.v3.exceptions;

/**
 * Thrown to indicate that the manager, store or reader the operation was on has been closed or deleted.
 * 
 * @author stevenso
 *
 */
public class SeqStoreClosedException extends SeqStoreIllegalStateException {
    private static final long serialVersionUID = 1L;

    public SeqStoreClosedException(String msg, Throwable ex) {
        super(msg, ex);
    }

    public SeqStoreClosedException(String msg) {
        super(msg);
    }

    public SeqStoreClosedException(Throwable ex) {
        super(ex);
    }
}
