package com.amazon.messaging.structuredIO;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import net.jcip.annotations.NotThreadSafe;

import lombok.SneakyThrows;


@NotThreadSafe
public class StructuredOutputImpl implements StructuredOutput {

    private final DataOutputStream dataOut;
    private final ByteArrayOutputStream bytes;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    
    public StructuredOutputImpl() {
        bytes = new ByteArrayOutputStream();
        dataOut = new DataOutputStream(bytes);
    }
    
    @Override
    @SneakyThrows(IOException.class)
    public void close() {
        if (!isClosed.compareAndSet(false,true))
            throw new IllegalStateException("Stream was already closed.");
        dataOut.close();
    }
    
    @Override
    @SneakyThrows(IOException.class)
    public void write(byte[] b) {
        dataOut.writeInt(b.length);
        dataOut.write(b);
    }

    @Override
    @SneakyThrows(IOException.class)
    public void write(byte[] b, int off, int len) {
        if (b.length-off < len)
            throw new IllegalArgumentException("Byte array is too short.");
        dataOut.writeInt(len);
        dataOut.write(b,off,len);
    }
    
    @Override
    @SneakyThrows(IOException.class)
    public void writeNested(StructuredOutput inner) {
        if (!inner.isClosed())
            throw new IllegalArgumentException("Cannot write a nested structure that is still open.");
        dataOut.writeInt(inner.length());
        inner.writeTo(dataOut);
    }

    @Override
    @SneakyThrows(IOException.class)
    public void writeBoolean(boolean v) {
        dataOut.writeBoolean(v);
    }

    @Override
    @SneakyThrows(IOException.class)
    public void writeByte(int v) {
        dataOut.writeByte(v);
    }

    @Override
    @SneakyThrows(IOException.class)
    public void writeChar(int v) {
        dataOut.writeChar(v);
    }

    @Override
    @SneakyThrows(IOException.class)
    public void writeDouble(double v) {
        dataOut.writeDouble(v);
    }

    @Override
    @SneakyThrows(IOException.class)
    public void writeFloat(float v) {
        dataOut.writeFloat(v);
    }

    @Override
    @SneakyThrows(IOException.class)
    public void writeInt(int v) {
        dataOut.writeInt(v);   
    }

    @Override
    @SneakyThrows(IOException.class)
    public void writeLong(long v) {
        dataOut.writeLong(v);
    }

    @Override
    @SneakyThrows(IOException.class)
    public void writeShort(int v) {
        dataOut.writeShort(v);
    }

    @Override
    @SneakyThrows(IOException.class)
    public void write(int b) {
        dataOut.write(b);
    }

    @Override
    public void writeBytes(String s) {
        this.write(s.getBytes());
    }

    @Override
    @SneakyThrows(IOException.class)
    public void writeChars(String s) {
        dataOut.writeInt(s.length());
        dataOut.writeChars(s);
    }

    @Override
    @SneakyThrows(IOException.class)
    public void writeUTF(String s) {
        dataOut.writeUTF(s);
    }

    @Override
    public boolean isClosed() {
        return isClosed.get();
    }

    @Override
    public int length() {
        return bytes.size();
    }

    @Override
    public byte[] getBytes() {
        return bytes.toByteArray();
    }

    @Override
    public void writeTo(OutputStream dumpTo) throws IOException {
        if (!isClosed.get())
            throw new IllegalStateException("Stream is not closed.");
        bytes.writeTo(dumpTo);
    }

    @Override
    public void writeString(String s) {
        this.writeChars(s);
    }
    
    @SneakyThrows(IOException.class)
    public void skipBytes(int numEmpty) {
        bytes.write(new byte[numEmpty]);
    }

}
