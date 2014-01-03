package com.amazon.messaging.seqstore.v3.exceptions;

/**
 * This is thrown to indicate the maximum inflight table size has been reached
 * 
 * @author stevenso
 *
 */
public class SeqStoreMaxInflightSizeReachedException extends SeqStoreException {
    private static final long serialVersionUID = 1L;

    public SeqStoreMaxInflightSizeReachedException(String storeName, String readerName, long max) {
        super("The max inflight size has been reached for " + storeName + "." + readerName + " at " + max + " messages");
    }
}
