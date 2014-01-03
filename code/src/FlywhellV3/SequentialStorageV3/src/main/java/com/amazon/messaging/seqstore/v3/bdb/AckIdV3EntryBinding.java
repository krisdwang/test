package com.amazon.messaging.seqstore.v3.bdb;

import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.je.DatabaseEntry;

public class AckIdV3EntryBinding implements EntryBinding<AckIdV3> {

    @Override
    public AckIdV3 entryToObject(DatabaseEntry entry) {
        return new AckIdV3(entry.getData());
    }

    @Override
    public void objectToEntry(AckIdV3 object, DatabaseEntry entry) {
        entry.setData(object.toBytes());
    }
}
