package com.amazon.messaging.seqstore.v3;

import edu.umd.cs.findbugs.annotations.NonNull;

public interface InflightInfoFactory<InfoType> {

	/**
	 * Get a (possibly new) message info object to associate with a dequeued
	 * message. This method must return a non-<code>null</code> message info
	 * object to associated with every message dequeued.
	 * 
	 * @param currentInfo
	 *     Current info object associated with the message, which is
	 *     <code>null</code> when a message is first added to in-flight from
	 *     the persistence layer.
	 * @return
	 *     New info object to associate with the message.
	 */
	@NonNull
	public InfoType getMessageInfoForDequeue(InfoType currentInfo);
	
	/**
	 * Get the timeout in milliseconds before this message will become
	 * available to be delivered again following a dequeue.
	 * 
	 * @param messageInfo
	 *     The message info associated with the message to be dequeued. This
	 *     is the info object returned by {@link #getMessageInfoForDequeue(Object)}
	 *     for this dequeue, as opposed to the info object passed to that
	 *     method.
	 * @return
	 *     Time period, in milliseconds, before the message next become
	 *     available for dequeue.
	 */
	public int getTimeoutForDequeue(InfoType messageInfo);
	
}
