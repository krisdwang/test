package com.amazon.messaging.structuredIO;

import java.io.DataInput;
import java.io.IOException;


public interface StructuredInput extends DataInput {

    byte[] readBytes() throws IOException;
    
    StructuredInput readNested() throws IOException;
    
    String readString() throws IOException;
}
