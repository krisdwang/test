package com.amazon.messaging.seqstore.v3;

import java.util.Collection;

import javax.annotation.concurrent.Immutable;


import lombok.ToString;

@Immutable
@ToString
public class Checkpoint<IdType extends AckId, LevelType extends AckId, InfoType> {
    private final String signature;
    
    private final LevelType ackLevel;
    private final LevelType readLevel;
    private final Collection<CheckpointEntryInfo<IdType, InfoType>> inflightMessages;
    
    public Checkpoint(String signature, LevelType ackLevel, LevelType readLevel,
                      Collection<CheckpointEntryInfo<IdType, InfoType>> inflightMessages) 
    {
        this.signature = signature;
        this.ackLevel = ackLevel;
        this.readLevel = readLevel;
        this.inflightMessages = inflightMessages;
    }

    /**
     * Get the signature for this checkpoint. This is used to ensure that checkpoint
     * provided is of the expected type and to support any needed backwards 
     * compatibility.
     * 
     * @return
     */
    public String getSignature() {
        return signature;
    }

    /**
     * Get the ack level for the checkpoint.
     */
    public LevelType getAckLevel() {
        return ackLevel;
    }
    
    /**
     * Get the read level for the checkpoint
     */
    public LevelType getReadLevel() {
        return readLevel;
    }
    
    /**
     * Get the list of inflight messages for the checkpoint. These are not guaranteed to be in
     * any order.
     */
    public Collection<CheckpointEntryInfo<IdType, InfoType>> getInflightMessages() {
        return inflightMessages;
    }
}
