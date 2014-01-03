package com.amazon.messaging.util;

import net.jcip.annotations.Immutable;
import lombok.Data;

import com.amazon.messaging.seqstore.v3.InflightInfoFactory;

@Immutable
@Data
public class BasicInflightInfoFactory implements InflightInfoFactory<BasicInflightInfo> {
	private final int redeliveryTimeout;
	
	public BasicInflightInfoFactory() {
		this.redeliveryTimeout = 60000;
	}
	
	public BasicInflightInfoFactory(int redeliveryTimeout) {
		this.redeliveryTimeout = redeliveryTimeout;
	}
	
	@Override
	public BasicInflightInfo getMessageInfoForDequeue(
			BasicInflightInfo currentInfo) {
		if (currentInfo == null) {
			return BasicInflightInfo.firstDequeueInfo();
		} else {
			return currentInfo.nextDequeueInfo();
		}
	}

	@Override
	public int getTimeoutForDequeue(BasicInflightInfo messageInfo) {
		return redeliveryTimeout;
	}
	
	
	public static final BasicInflightInfoFactory DEFAULT_INSTANCE =
		new BasicInflightInfoFactory();
}
