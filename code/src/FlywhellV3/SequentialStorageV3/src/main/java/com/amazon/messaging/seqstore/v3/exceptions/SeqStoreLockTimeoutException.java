package com.amazon.messaging.seqstore.v3.exceptions;

/**
 * This exception is thrown when it takes to long to get a lock for some action
 *  
 * @author stevenso
 *
 */
public class SeqStoreLockTimeoutException extends SeqStoreException {
    private static final long serialVersionUID = 1L;

    public SeqStoreLockTimeoutException(String msg, Throwable ex) {
        super(msg, ex);
    }

    public SeqStoreLockTimeoutException(String msg) {
        super(msg);
    }

    public SeqStoreLockTimeoutException(Throwable ex) {
        super(ex);
    }
}
