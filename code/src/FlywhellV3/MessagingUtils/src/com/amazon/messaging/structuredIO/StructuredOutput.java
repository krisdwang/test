package com.amazon.messaging.structuredIO;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;


public interface StructuredOutput extends DataOutput {

    void writeNested(StructuredOutput inner);
    
    boolean isClosed();
    
    int length();
    
    void close();
    
    byte[] getBytes();
    
    void writeTo(OutputStream dumpTo) throws IOException;
    
    /**
     * Alias of writeChars. (Preferred method or writing strings).
     */
    void writeString(String s);

    @Override
    public void write(int b);

    @Override
    public void write(byte[] b);

    @Override
    public void write(byte[] b, int off, int len);

    @Override
    public void writeBoolean(boolean v);

    @Override
    public void writeByte(int v);

    @Override
    public void writeBytes(String s);

    @Override
    public void writeChar(int v);

    @Override
    public void writeChars(String s);

    @Override
    public void writeDouble(double v);

    @Override
    public void writeFloat(float v);

    @Override
    public void writeInt(int v);

    @Override
    public void writeLong(long v);

    @Override
    public void writeShort(int v);

    @Override
    public void writeUTF(String s);
    
}
