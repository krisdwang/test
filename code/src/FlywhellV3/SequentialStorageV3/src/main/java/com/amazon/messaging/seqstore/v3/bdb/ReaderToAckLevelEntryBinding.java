package com.amazon.messaging.seqstore.v3.bdb;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

public class ReaderToAckLevelEntryBinding extends TupleBinding<Map<String, AckIdV3>> {
    @Override
    public Map<String, AckIdV3> entryToObject(TupleInput input) {
        Map<String, AckIdV3> result = new HashMap<String, AckIdV3>();
        while (input.available() > 0) {
            String readerName = input.readString();
            AckIdV3 ackLevel = BindingUtils.readAckIdV3(input);
            result.put(readerName, ackLevel);
        }
        return Collections.unmodifiableMap( result );
    }

    @Override
    public void objectToEntry(Map<String, AckIdV3> object, TupleOutput output) {
        for (Map.Entry<String, AckIdV3> reader : object.entrySet()) {
            output.writeString(reader.getKey());
            BindingUtils.writeAckIdV3(output, reader.getValue());
        }
    }
}
