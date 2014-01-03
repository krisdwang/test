package com.amazon.messaging.seqstore.v3.internal;

import lombok.EqualsAndHashCode;

import com.amazon.messaging.seqstore.v3.Entry;
import com.amazon.messaging.seqstore.v3.StoredEntry;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

@EqualsAndHashCode(exclude="payload",callSuper=false)
public class StoredEntryV3 extends StoredEntry<AckIdV3> {

    private final AckIdV3 ackId;

    private final byte[] payload;

    private final String logId_;

    public StoredEntryV3(AckIdV3 id, Entry message) {
        this(id, message.getPayload(), message.getLogId());
    }

    @SuppressWarnings("EI_EXPOSE_REP2")
    public StoredEntryV3(AckIdV3 id, byte[] message, String logId) {
        ackId = id;
        payload = message;
        logId_ = logId;
    }

    @Override
    public AckIdV3 getAckId() {
        return ackId;
    }

    @Override
    public long getAvailableTime() {
        return ackId.getTime();
    }

    @Override
    @SuppressWarnings("EI_EXPOSE_REP")
    public byte[] getPayload() {
        return payload;
    }

    @Override
    public String getLogId() {
        return logId_;
    }

}
