package com.amazon.messaging.seqstore.v3.bdb;

import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.StoredEntryV3;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

public class StoredEntryV3EntryBinding extends TupleBinding<StoredEntry<AckIdV3>> {
    @Override
    public StoredEntry<AckIdV3> entryToObject(TupleInput input) {
        AckIdV3 ackId = BindingUtils.readAckIdV3(input);
        byte[] payload;
        payload = BindingUtils.readBytes(input);
        String logId = input.readString();
        return new StoredEntryV3(ackId, payload, logId);
    }

    @Override
    public void objectToEntry(StoredEntry<AckIdV3> object, TupleOutput output) {
        BindingUtils.writeAckIdV3(output, object.getAckId());
        BindingUtils.writeBytes(output, object.getPayload());
        output.writeString(object.getLogId());
    }
    
    /**
     * Return a tuple that is sized right to contain the given entry. This can avoid a lot of array copies.
     */
    @Override
    protected TupleOutput getTupleOutput(StoredEntry<AckIdV3> object) {
        int byteLength = 4 + object.getPayloadSize() + 4 + object.getAckId().getSerializedLength();
        if( object.getLogId() != null ) byteLength += object.getLogId().length() + 1; // Assume usually plain ASCII log ids.
        else byteLength += 2; // 0xFF special character + null terminator
        return new TupleOutput( new byte[byteLength] );
    }
}
