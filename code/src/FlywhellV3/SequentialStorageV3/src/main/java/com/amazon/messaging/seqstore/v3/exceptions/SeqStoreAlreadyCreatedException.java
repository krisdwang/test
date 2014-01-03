package com.amazon.messaging.seqstore.v3.exceptions;

/**
 * An exception throw to indicate that there was an attempt to create an already existing object
 * @author stevenso
 *
 */
public class SeqStoreAlreadyCreatedException extends SeqStoreIllegalStateException {
    private static final long serialVersionUID = 1L;

    public SeqStoreAlreadyCreatedException(String msg, Throwable ex) {
        super(msg, ex);
    }

    public SeqStoreAlreadyCreatedException(String msg) {
        super(msg);
    }

    public SeqStoreAlreadyCreatedException(Throwable ex) {
        super(ex);
    }

}
