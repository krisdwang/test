package com.amazon.messaging.seqstore.v3.exceptions;

import lombok.ToString;

/**
 * This exception is thrown when the data store gives an unrecoverable error. In response to this exception
 * the calling application should shutdown the SeqStore or FlywheelManager.
 * 
 * @author stevenso
 *
 */
@ToString(includeFieldNames=true,callSuper=true)
public class SeqStoreUnrecoverableDatabaseException extends SeqStoreDatabaseException {
    private static final long serialVersionUID = 1L;
    
    private final String reason;

    public SeqStoreUnrecoverableDatabaseException(String reason, String msg, Throwable ex) {
        super(msg, ex);
        this.reason = reason;
    }

    public SeqStoreUnrecoverableDatabaseException(String reason, String msg) {
        super(msg);
        this.reason = reason;
    }
    
    public String getReason() {
        return reason;
    }
}
