package com.amazon.messaging.seqstore.v3;

import net.jcip.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.NonNull;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Immutable
@Data
@EqualsAndHashCode(callSuper=false)
public class InflightEntry<IdType extends AckId, InfoType> extends StoredEntry<IdType> {
    @NonNull
    private final StoredEntry<IdType> storedEntry;
    
    private final InfoType inflightInfo;
    
    private final long delayUntilNextRedrive;
    
    public InflightEntry(StoredEntry<IdType> storedEntry, InflightEntryInfo<IdType, InfoType> entryInfo) {
    	this.storedEntry = storedEntry;
    	this.inflightInfo = entryInfo.getInflightInfo();
    	this.delayUntilNextRedrive = entryInfo.getDelayUntilNextRedrive();
    }
    
    public InflightEntry(StoredEntry<IdType> storedEntry, InfoType inflightInfo,
    		long delayUntilNextRedrive) {
    	this.storedEntry = storedEntry;
    	this.inflightInfo = inflightInfo;
    	this.delayUntilNextRedrive = delayUntilNextRedrive;
    }

    @Override
    public IdType getAckId() {
        return storedEntry.getAckId();
    }

    @Override
    public long getAvailableTime() {
        return storedEntry.getAvailableTime();
    }

    @Override
    public String getLogId() {
        return storedEntry.getLogId();
    }

    @Override
    public byte[] getPayload() {
        return storedEntry.getPayload();
    }

    @Override
    public int getPayloadSize() {
        return storedEntry.getPayloadSize();
    }
}
