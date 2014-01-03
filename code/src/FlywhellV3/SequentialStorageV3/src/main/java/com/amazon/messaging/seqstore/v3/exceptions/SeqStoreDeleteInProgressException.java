package com.amazon.messaging.seqstore.v3.exceptions;

/**
 * This exception is thrown to indicate that an object can't be recreated yet because the delete
 * of the previous version of the object is still in progress.
 * @author stevenso
 *
 */
public class SeqStoreDeleteInProgressException extends SeqStoreException {
    private static final long serialVersionUID = 1L;

    public SeqStoreDeleteInProgressException(String msg, Throwable ex) {
        super(msg, ex);
    }

    public SeqStoreDeleteInProgressException(String msg) {
        super(msg);
    }

    public SeqStoreDeleteInProgressException(Throwable ex) {
        super(ex);
    }

}
