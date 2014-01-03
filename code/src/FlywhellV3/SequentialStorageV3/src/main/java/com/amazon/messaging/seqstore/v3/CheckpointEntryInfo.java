package com.amazon.messaging.seqstore.v3;

import javax.annotation.concurrent.Immutable;

import lombok.Data;
import lombok.EqualsAndHashCode;
import edu.umd.cs.findbugs.annotations.NonNull;

@Immutable
@Data
@EqualsAndHashCode(callSuper=false)
public class CheckpointEntryInfo<IdType extends AckId, InfoType> {
    @NonNull
    private final IdType ackId;
    
    private final InfoType inflightInfo;
    
    /**
     * The next redrive time of the message. Note that this may not be relative
     * to the System.currentTimeMillis as the clock used is the estimated clock
     * for the host that enqueued the message.
     */
    private final long nextRedriveTime;
}