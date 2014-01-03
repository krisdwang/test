package com.amazon.messaging.seqstore.v3;

import com.amazon.messaging.seqstore.v3.Entry;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

public class TestEntry extends Entry {

    private String name;

    private final byte[] payload;
    
    private final long availableTime;

    @Override
    public String getLogId() {
        return name;
    }

    @Override
    @SuppressWarnings("EI_EXPOSE_REP")
    public byte[] getPayload() {
        return payload;
    }
    
    @Override
    public long getAvailableTime() {
        return availableTime;
    }
    
    @SuppressWarnings("EI_EXPOSE_REP2")
    public TestEntry(byte[] payload, long availableTime, String name) {
        super();
        this.name = name;
        this.payload = payload;
        this.availableTime = availableTime;
    }

    @SuppressWarnings("EI_EXPOSE_REP2")
    public TestEntry(byte[] payload, String name) {
        this( payload, 1, name );
    }

    @SuppressWarnings("EI_EXPOSE_REP2")
    public TestEntry(byte[] bytes) {
        this(bytes,null);
    }

    public TestEntry(String name) {
        this(name.getBytes(), name);
    }

    public TestEntry(String payload, String name) {
        this( payload.getBytes(), name );
    }
    
    public TestEntry(String name, long availableTime) {
        this(name.getBytes(), availableTime, name );
    }

    public TestEntry(String payload, String name, long availableTime) {
        this( payload.getBytes(), availableTime,  name );
    }
}
