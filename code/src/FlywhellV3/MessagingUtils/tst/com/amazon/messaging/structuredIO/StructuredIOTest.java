package com.amazon.messaging.structuredIO;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.Test;


public class StructuredIOTest {

    @Test(expected=IOException.class)
    public void testCreateEmpty() throws IOException {
        StructuredOutputImpl out = new StructuredOutputImpl();
        out.close();
        StructuredInputmpl in = new StructuredInputmpl(out.getBytes(), 0, out.length());
        in.readInt();
        fail();
    }

    
    @Test
    public void testBasicTypes() throws IOException {
        StructuredOutputImpl out = new StructuredOutputImpl();
        out.write(10);
        out.writeInt(-2);
        out.writeFloat(3.5f);
        out.writeString("Hello world.");
        out.close();
        StructuredInputmpl in = new StructuredInputmpl(out.getBytes(), 0, out.length());
        assertEquals((byte)10,in.readByte());
        assertEquals(-2, in.readInt());
        assertEquals(3.5f, in.readFloat(),0);
        assertEquals("Hello world.", in.readString());
    }
    
    @Test
    public void testExotic() throws IOException {
        StructuredOutputImpl out = new StructuredOutputImpl();
        out.write(Byte.MIN_VALUE);
        out.writeInt(Integer.MAX_VALUE);
        out.writeFloat(Float.NaN);
        out.writeString("Hello \0 world.");
        out.close();
        StructuredInputmpl in = new StructuredInputmpl(out.getBytes(), 0, out.length());
        assertEquals(Byte.MIN_VALUE,in.readByte());
        assertEquals(Integer.MAX_VALUE, in.readInt());
        assertEquals(Float.NaN, in.readFloat(),0);
        assertEquals("Hello \0 world.", in.readString());
    }
    
}
