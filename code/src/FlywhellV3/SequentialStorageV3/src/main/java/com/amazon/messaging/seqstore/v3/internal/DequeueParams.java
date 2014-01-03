package com.amazon.messaging.seqstore.v3.internal;

import lombok.Data;

@Data
public class DequeueParams<InfoType> {
	private final InfoType inflightInfo;
	private final int timeout;
}
