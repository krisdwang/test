package com.amazon.messaging.seqstore.v3.exceptions;


/**
 * Used to indicate that Enqueues are disabled (either because user disabled
 * them OR an out of disk space was detected by the monitor)
 * 
 * @author Harshawardhan Gadgil hgadgil@amazon.com<br>
 *         Created: Oct 12, 2007
 */
public class EnqueuesDisabledException extends SeqStoreException {
    private static final long serialVersionUID = 1686375617218479384L;

    /**
     * @param msg
     */
    public EnqueuesDisabledException(String msg) {
        super(msg);
    }
}
