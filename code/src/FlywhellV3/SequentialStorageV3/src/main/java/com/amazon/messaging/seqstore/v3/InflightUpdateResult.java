package com.amazon.messaging.seqstore.v3;

/**
 * Result of a in-flight message update operation.
 */
public enum InflightUpdateResult {
    /**
     * the message was never dequeued or already acknowledged.
     */
    NOT_FOUND,

    /**
     * the value of the expect parameter did not match (==) the
     * current message info object.
     */
    EXPECTATION_UNMET,

    /**
     * The update was completed successfully w/o problems.
     */
    DONE
}