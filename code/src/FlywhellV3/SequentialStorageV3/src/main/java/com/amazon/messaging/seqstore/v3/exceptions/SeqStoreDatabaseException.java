package com.amazon.messaging.seqstore.v3.exceptions;

/**
 * Used to report problems with the database. 
 * 
 * @author stevenso
 *
 */
public class SeqStoreDatabaseException extends SeqStoreException {
    private static final long serialVersionUID = 1L;

    public SeqStoreDatabaseException(String msg, Throwable ex) {
        super(msg, ex);
    }

    public SeqStoreDatabaseException(String msg) {
        super(msg);
    }

    public SeqStoreDatabaseException(Throwable ex) {
        super(ex);
    }
}
