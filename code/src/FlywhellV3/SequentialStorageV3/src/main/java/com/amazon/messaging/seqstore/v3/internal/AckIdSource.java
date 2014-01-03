package com.amazon.messaging.seqstore.v3.internal;

public interface AckIdSource {
    /**
     * Request a new AckIdV3 to be used for enqueuing into the store. The store should 
     * not allow dequeues to advance beyond this point until {@link #enqueueFinished(AckIdV3)} 
     * is called with the returned AckIdV3.
     *  
     * @param requestedTime the requested time for the enqueue. The implementation of AckIdSource
     *  may return an AckIdV3 with a later time than the requested time.
     * @return
     */
    public AckIdV3 requestNewId(long requestedTime);
    
    /**
     * Record that a message is being enqueued with the specified id. Should return
     * true for ids created using {@link #requestNewId(long)}   
     * 
     * @param ackId
     * @return true if the new ID can be used, false otherwise.
     */
    public boolean recordNewId(AckIdV3 ackId);
    
    /**
     * Return true if the ackId was handed out by {@link #requestNewId(long)} but has not yet
     * been recorded as having completed using {@link #enqueueFinished(AckIdV3)}.
     * 
     * @param ackId
     * @return true if the ackId was handed out but has not been recorded as having finished, else false.
     */
    public boolean isUnfinished(AckIdV3 ackId);
    
    /**
     * Note than the enqueue with the given AckIdV3 has finished. This does not mean the enqueue
     * was successful, just that no more work will be done for this enqueue.
     * 
     * @param ackId
     */
    public void enqueueFinished(AckIdV3 ackId);
    
    /**
     * Return the minimum AckIdV3 allowed for incomplete and new enqueues. No lower id will
     * be unfinished or handed out by {@link #requestNewId(long)}. {@link #recordNewId(AckIdV3)}
     * should always return false for any ids below this level.
     * 
     * @return
     */
    public AckIdV3 getMinEnqueueLevel();

    /**
     * Get the AckIdV3 of the smallest AckIdV3 returned by {@link #requestNewId(long)} that 
     * has not been recorded as having been finished using {@link #enqueueFinished(AckIdV3)}.
     * 
     * @return the smallest AckIdV3 handed out that was no record as having been finished, or null
     *  if there is no such AckIdV3.
     */
    public AckIdV3 getMinimumUnfinishedEnqueue();

    /**
     * Return true if there is at least one unfinished enqueue.
     * 
     * @return true if there is an unfinished enqueue, else false
     */
    public boolean hasUnfinishedEnqueue();
}
