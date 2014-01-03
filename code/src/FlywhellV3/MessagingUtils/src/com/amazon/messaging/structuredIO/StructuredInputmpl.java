package com.amazon.messaging.structuredIO;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import net.jcip.annotations.NotThreadSafe;


import edu.umd.cs.findbugs.annotations.SuppressWarnings;

@NotThreadSafe
public class StructuredInputmpl implements StructuredInput {

    private final byte[] buffer;
    private final DataInputStream dataIn;
    private final int offset;
    private final int length;

    @SuppressWarnings("EI_EXPOSE_REP2")
    public StructuredInputmpl (byte[] in, int offset, int length) {
        this.buffer = in;
        this.offset = offset;
        this.length = length;
        if (offset + length > in.length ) 
            throw new IllegalArgumentException("Input buffer is too short.");
        ByteArrayInputStream bytes = new ByteArrayInputStream(in, offset,length);
        dataIn = new DataInputStream(bytes);
    }
    
    @Override
    public boolean readBoolean() throws IOException {
        return dataIn.readBoolean();
    }

    @Override
    public byte readByte() throws IOException {
        return dataIn.readByte();
    }

    @Override
    public byte[] readBytes() throws IOException {
        int size = dataIn.readInt();
        byte[] result = new byte[size];
        int read = dataIn.read(result);
        if (read != size && !(read == -1 && size == 0))
            throw new IOException("Length read differs from stated length.");
        return result;
    }
    
    public int findPos() throws IOException {
        int remaining = dataIn.available();
        return offset + length - remaining;
    }

    @Override
    public StructuredInput readNested() throws IOException {
        int size = dataIn.readInt();
        StructuredInputmpl nested = new StructuredInputmpl(buffer, findPos(),size);
        long skipped = dataIn.skip(size);
        if (skipped != size&& !(skipped == -1 && size == 0))
            throw new IOException("Length read differs from stated length.");
        return nested;
    }

    @Override
    public char readChar() throws IOException {
        return dataIn.readChar();
    }

    @Override
    public double readDouble()  throws IOException {
        return dataIn.readDouble();
    }

    @Override
    public float readFloat()  throws IOException {
        return dataIn.readFloat();
    }

    @Override
    public int readInt()  throws IOException {
        return dataIn.readInt();
    }

    @Override
    public long readLong()  throws IOException {
        return dataIn.readLong();
    }

    @Override
    public short readShort() throws IOException  {
        return dataIn.readShort();
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        dataIn.readFully(b);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        dataIn.readFully(b,off,len);        
    }

    @Override
    public String readLine() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readUTF() throws IOException {
        return dataIn.readUTF();
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return dataIn.readUnsignedByte();
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return dataIn.readUnsignedShort();
    }

    @Override
    public int skipBytes(int n) throws IOException {
        return dataIn.skipBytes(n);
    }

    @Override
    public String readString() throws IOException {
        int size = dataIn.readInt();
        StringBuffer b = new StringBuffer(size);
        for (int i = 0; i<size;i++) {
            b.append(dataIn.readChar());
        }
        return b.toString();
    }

}
