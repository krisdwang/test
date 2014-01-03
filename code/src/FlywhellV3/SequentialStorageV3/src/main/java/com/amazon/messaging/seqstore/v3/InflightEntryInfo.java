package com.amazon.messaging.seqstore.v3;

import edu.umd.cs.findbugs.annotations.NonNull;
import lombok.Data;
import lombok.EqualsAndHashCode;
import net.jcip.annotations.Immutable;

@Immutable
@Data
@EqualsAndHashCode(callSuper=false)
public class InflightEntryInfo<IdType extends AckId, InfoType> {
	@NonNull
	private final IdType ackId;
	
	private final InfoType inflightInfo;
	
	private final long delayUntilNextRedrive;
}
