package com.amazon.messaging.util;

import lombok.Data;
import net.jcip.annotations.Immutable;

@Immutable
@Data
public class BasicInflightInfo {
	private final int deliveryCount;
	
	private static final BasicInflightInfo FIRST_DEQUEUE_INFO =
		new BasicInflightInfo(1);
	
	public static BasicInflightInfo firstDequeueInfo() {
		return FIRST_DEQUEUE_INFO;
	}
	
	public BasicInflightInfo nextDequeueInfo() {
		return new BasicInflightInfo(deliveryCount + 1);
	}
}
