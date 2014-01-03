package com.amazon.dse.messaging.datagram;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.Vector;

import org.junit.Test;

import com.amazon.dse.messaging.datagramConverter.DecodingException;
import com.amazon.dse.messaging.datagramConverter.EncodingException;
import com.amazon.ion.IonList;
import com.amazon.ion.IonLoader;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.streaming.IonBinaryWriter;
import com.amazon.ion.streaming.IonIterator;
import com.amazon.ion.system.SystemFactory;

public class BasicTests {

    IonDatagramConverter conv = new IonDatagramConverter();
    IonTypedCreator checker = new IonTypedCreator();
    
    @Test
    public void testEmpty() {
        IonBinaryWriter wr = new IonBinaryWriter();
        byte[] buffer = null;

        try {
            wr.startStruct();
            wr.closeStruct();
            buffer = wr.getBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        IonReader ir = IonIterator.makeIterator(buffer);
        if (ir.hasNext()) {
            ir.next();
            ir.stepInto();
            assertEquals(false, ir.hasNext());
            ir.stepOut();
            assertEquals(false, ir.hasNext());
        }        
       
    }
    
    
    @Test
    public void testFooBar() {
        IonBinaryWriter wr = new IonBinaryWriter();
        byte[] buffer = null;

        try {
            wr.startStruct();
            wr.writeFieldname("Foo");
            wr.writeString("Bar");
            wr.closeStruct();
            buffer = wr.getBytes();
        } catch (IOException e) {

            throw new RuntimeException(e);
        }

        IonReader ir = IonIterator.makeIterator(buffer);
        if (ir.hasNext()) {
            ir.next();
            ir.stepInto();
            while (ir.hasNext()) {
                assertEquals(ir.next(), IonType.STRING);
                assertEquals(ir.getFieldName(), "Foo");
                assertEquals(ir.getString(), "Bar");
            }
        }
        IonSystem sys = SystemFactory.newSystem();
        IonValue val = sys.singleValue(buffer);
        IonStruct s = sys.newEmptyStruct();
        s.put("Foo", sys.newString("Bar"));
        assertEquals(s.toString(), val.toString());
    }

    @Test
    public void testBoolean() {
        IonBinaryWriter wr = new IonBinaryWriter();
        byte[] buffer = null;

        try {
            wr.startStruct();
            wr.writeFieldname("Foo");
            wr.addAnnotation("boolean");
            wr.writeBool(true);
            wr.closeStruct();
            buffer = wr.getBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        IonReader ir = IonIterator.makeIterator(buffer);
        if (ir.hasNext()) {
            ir.next();
            ir.stepInto();
            while (ir.hasNext()) {
                assertEquals(ir.next(), IonType.BOOL);
                assertEquals(ir.getFieldName(), "Foo");
                String[] annotations = ir.getAnnotations();
                assertTrue(annotations != null && annotations.length == 1);
                assertTrue("boolean".equals(annotations[0]));
                assertEquals(ir.getBool(), true);
            }
        }
    }

    @Test
    public void testStreamingIon() throws IOException {
        IonSystem sys = SystemFactory.newSystem();
        //        IonLoader loader = sys.newLoader();
        IonStruct s = sys.newNullStruct();
        for (int i = 0; i < 5; i++) {
            IonList l = sys.newNullList();
            for (int j = 0; j < 5; j++) {
                l.add(sys.newString("Hello: " + j));
            }
            s.add("HashKey " + i, l);
        }
        s.add("Foo", sys.newString("Bar"));
        //s.add("self", s);
        // loader.load();
        byte[] buffer = sys.newDatagram(s).toBytes();

        IonBinaryWriter wr = new IonBinaryWriter();
        wr.startStruct();
        wr.writeFieldname("Foo");
        wr.writeString("Bar");
        wr.closeStruct();
        buffer = wr.getBytes();

        IonReader ir = IonIterator.makeIterator(buffer);
        if (ir.hasNext()) {
            ir.next(); /// need to call next
            ir.stepInto();
            while (ir.hasNext()) {
                System.out.println(ir.next() + "\t" + ir.getFieldName());
            }
        }

    }

    //Test Sample map.
    //{hello=true, Almost Done.=true, This is a test String.=true, 12242.124598129=12242.124598129, Something=null, false=false, true=true, long=9326, 12=-12}
    @Test
    public void testSampleMap() {
        IonBinaryWriter wr = new IonBinaryWriter();
        byte[] buffer = null;

        try {
            wr.startStruct();
            wr.writeFieldname("hello");
            wr.writeBool(true);
            wr.writeFieldname("Almost Done.");
            wr.writeBool(true);
            wr.writeFieldname("This is a test String.");
            wr.writeBool(true);
            wr.writeFieldname("12242.124598129");
            wr.writeFloat(12242.124598129);
            wr.writeFieldname("Something");
            wr.writeNull();
            wr.writeFieldname("false");
            wr.writeBool(false);
            wr.writeFieldname("true");
            wr.writeBool(true);
            wr.writeFieldname("long");
            wr.writeInt((long) 9326);
            wr.writeFieldname("12");
            wr.writeInt(-12);
            wr.closeStruct();
            buffer = wr.getBytes();
        } catch (IOException e) {

            throw new RuntimeException(e);
        }

        IonReader ir = IonIterator.makeIterator(buffer);
        assertTrue(ir.hasNext());
        ir.next();
        ir.stepInto();
        assertTrue(ir.hasNext());
        assertEquals(ir.next(), IonType.BOOL);
        assertEquals(ir.getFieldName(), "hello");
        assertEquals(ir.getBool(), true);
        assertTrue(ir.hasNext());
        assertEquals(ir.next(), IonType.BOOL);
        assertEquals(ir.getFieldName(), "Almost Done.");
        assertEquals(ir.getBool(), true);
        assertTrue(ir.hasNext());
        assertEquals(ir.next(), IonType.BOOL);
        assertEquals(ir.getFieldName(), "This is a test String.");
        assertEquals(ir.getBool(), true);
        assertTrue(ir.hasNext());
        assertEquals(ir.next(), IonType.FLOAT);
        assertEquals(ir.getFieldName(), "12242.124598129");
        assertEquals(ir.getDouble(), 12242.124598129);
        assertTrue(ir.hasNext());
        assertEquals(ir.next(), IonType.NULL);
        assertEquals(ir.getFieldName(), "Something");
        assertTrue(ir.isNull());
        assertTrue(ir.hasNext());
        assertEquals(ir.next(), IonType.BOOL);
        assertEquals(ir.getFieldName(), "false");
        assertEquals(ir.getBool(), false);
        assertTrue(ir.hasNext());
        assertEquals(ir.next(), IonType.BOOL);
        assertEquals(ir.getFieldName(), "true");
        assertEquals(ir.getBool(), true);
        assertTrue(ir.hasNext());
        assertEquals(ir.next(), IonType.INT);
        assertEquals(ir.getFieldName(), "long");
        assertEquals(ir.getLong(), (long) 9326);
        assertTrue(ir.hasNext());
        assertEquals(ir.next(), IonType.INT);
        assertEquals(ir.getFieldName(), "12");
        assertEquals(ir.getInt(), -12);
        assertFalse(ir.hasNext());
        ir.stepOut();
        assertFalse(ir.hasNext());
    }

    //Test Sample map.
    //{long=9326, 12=-12}
    @Test
    public void testExtraHasNext() {
        IonWriter wr = new IonBinaryWriter();
        byte[] buffer = null;

        try {
            wr.startStruct();
            wr.writeFieldname("long");
            wr.writeInt((long) 9326);
            wr.writeFieldname("12");
            wr.writeInt(-12);
            wr.closeStruct();
            buffer = wr.getBytes();
        } catch (IOException e) {

            throw new RuntimeException(e);
        }

        IonReader ir = IonIterator.makeIterator(buffer);
        if (ir.hasNext()) {
            ir.next();
            ir.stepInto();
            assertTrue(ir.hasNext());
            assertEquals(ir.next(), IonType.INT);
            assertTrue(ir.hasNext());
            assertEquals(ir.getFieldName(), "long");
            assertEquals(ir.getLong(), (long) 9326);
            assertTrue(ir.hasNext());
            assertEquals(ir.next(), IonType.INT);
            assertEquals(ir.getFieldName(), "12");
            assertEquals(ir.getInt(), -12);
        }
    }

    @Test
    public void testBenchmarkMap() throws IOException, EncodingException, DecodingException {
        IonDatagramConverter conv = new IonDatagramConverter();
        Map m = new TreeMap();

        m.put("This is a test String.", true);
        m.put("true", true);
        m.put("false", false);
        m.put("12242.124598129", 12242.124598129);
        m.put("long",(long) 9326);
        m.put("12", -12);
        m.put("Almost Done.", true);

        byte[] bytes = conv.serialize(m);

        Map m2 = conv.deserialize(bytes);

        IonSystem sys = SystemFactory.newSystem();
  //      IonValue val = sys.singleValue(bytes);


        assertTrue(m2.containsKey("This is a test String."));// header
        assertEquals(m2.get("true"),true);// true
        assertEquals(m2.get("false"),false);// false
        assertEquals(m2.get("12242.124598129"),12242.124598129);// double
        assertEquals(m2.get("long"),(long) 9326);// long
        assertEquals(m2.get("12"),-12);// int
        assertEquals(m2.get("Almost Done."),true);// string

    }
    
    @Test
    public void testArrays() throws EncodingException, DecodingException {
        boolean [] bools = new boolean[]{true,false,false,true};
        byte [] bytes = new byte[]{-1,0,-2,1,Byte.MAX_VALUE,Byte.MIN_VALUE};
        short [] shorts = new short[]{-1,0,-2,1,Short.MAX_VALUE,Short.MIN_VALUE};
        int [] ints = new int[]{-1,0,-2,1,Integer.MAX_VALUE,Integer.MIN_VALUE};
        long [] longs = new long[]{-1,0,-2,1,Long.MAX_VALUE,Long.MIN_VALUE};
        Boolean [] Bools = new Boolean[]{null,true,false,false,true};
        Byte [] Bytes = new Byte[]{null,-1,0,-2,1,Byte.MAX_VALUE,Byte.MIN_VALUE};
        Short [] Shorts = new Short[]{null,-1,0,-2,1,Short.MAX_VALUE,Short.MIN_VALUE};
        Integer [] Ints = new Integer[]{null,-1,0,-2,1,Integer.MAX_VALUE,Integer.MIN_VALUE};
        Long [] Longs = new Long[]{null,-1L,0L,-2L,1L,Long.MAX_VALUE,Long.MIN_VALUE};
//        BigInteger [] bigInts = new BigInteger[]{
//                null,
//                new BigInteger(bytes),
//                new BigInteger(new byte[]{-1,0,-2,1,Byte.MAX_VALUE,Byte.MIN_VALUE,-1,0,-2,1,Byte.MAX_VALUE,Byte.MIN_VALUE})
//        };
        Float [] Floats = new Float[]{-1f,0f,-2f,1f,(float)Long.MAX_VALUE,(float)Long.MIN_VALUE,Float.MAX_VALUE,0x1.0p-126f,Float.MIN_VALUE,Float.NaN,Float.NEGATIVE_INFINITY,Float.POSITIVE_INFINITY};
        Double [] Doubles = new Double[]{-1d,0d,-2d,1d,(double)Long.MAX_VALUE,(double)Long.MIN_VALUE,Double.MAX_VALUE,0x1.0p-1022,Double.MIN_VALUE,Double.NaN,Double.NEGATIVE_INFINITY,Double.POSITIVE_INFINITY};
        float [] floats = new float[]{-1,0,-2,1,Long.MAX_VALUE,Long.MIN_VALUE,Float.MAX_VALUE,0x1.0p-126f,Float.MIN_VALUE,Float.NaN,Float.NEGATIVE_INFINITY,Float.POSITIVE_INFINITY};
        double [] doubles = new double[]{-1,0,-2,1,Long.MAX_VALUE,Long.MIN_VALUE,Double.MAX_VALUE,0x1.0p-1022,Double.MIN_VALUE,Double.NaN,Double.NEGATIVE_INFINITY,Double.POSITIVE_INFINITY};
//        BigDecimal [] bigDecimals = new BigDecimal[]{
//                null,
//                new BigDecimal(new BigInteger(bytes)),
//                (new BigDecimal(Long.MAX_VALUE)),
//                new BigDecimal(Long.MIN_VALUE),
//                new BigDecimal(0),
//                new BigDecimal(-1),
//                new BigDecimal(2),
//                new BigDecimal(-2),
//                new BigDecimal(Double.MIN_VALUE),
//                new BigDecimal(Double.MAX_VALUE),
//                BigDecimal.TEN,
//                BigDecimal.ONE,
//                BigDecimal.ZERO
//        };
        TreeMap map = new TreeMap();
        ArrayList list = new ArrayList();
        list.add(bools);
        list.add(bytes);
        list.add(shorts);
        list.add(ints);
        list.add(longs);
        list.add(Bools);
        list.add(Bytes);
        list.add(Shorts);
        list.add(Ints);
        list.add(Longs);
//        list.add(bigInts);
        list.add(floats);
        list.add(doubles);
        list.add(Floats);
        list.add(Doubles);
//        list.add(bigDecimals);
        map.put("list",list);
        map.put("bools", bools);
        map.put("bytes",bytes);
        map.put("shorts",shorts);
        map.put("ints",ints);
        map.put("longs",longs);
        map.put("Bools", Bools);
        map.put("Bytes",Bytes);
        map.put("Shorts",Shorts);
        map.put("Ints",Ints);
        map.put("Longs",Longs);
//        map.put("bigInts",bigInts);
        map.put("floats",floats);
        map.put("doubles", doubles);
        map.put("Floats",Floats);
        map.put("Doubles", Doubles);
//        map.put("bigDecimals",bigDecimals);
        byte[] result = conv.serialize(map);
        Map out = conv.deserialize(result);
        assertTrue(checker.areEqual(map, out));
    }
    
    @Test
    public void testBenchmarkDirect() throws IOException, EncodingException, DecodingException {

        byte[] bytes ;
        
        IonWriter wr = new IonBinaryWriter();
        wr.startStruct();
        
        wr.writeFieldname("12");
        wr.addAnnotation(int.class.getCanonicalName());
        wr.writeInt(-12);
        wr.writeFieldname("12242.124598129");
        wr.addAnnotation(double.class.getCanonicalName());
        wr.writeFloat(12242.124598129);
        wr.writeFieldname("Almost Done.");
        wr.addAnnotation(boolean.class.getCanonicalName());
        wr.writeBool(true);
        wr.writeFieldname("This is a test String.");
        wr.addAnnotation(boolean.class.getCanonicalName());
        wr.writeBool(true);
        wr.writeFieldname("false");
        wr.addAnnotation(boolean.class.getCanonicalName());
        wr.writeBool(false);
        wr.writeFieldname("long");
        wr.addAnnotation(long.class.getCanonicalName());
        wr.writeInt((long) 9326);
        wr.writeFieldname("true");
        wr.addAnnotation(boolean.class.getCanonicalName());
        wr.writeBool(true);
        wr.closeStruct();
        
        bytes = wr.getBytes();
        
        IonReader ir = IonIterator.makeIterator(bytes);
        assertTrue(ir.hasNext());
        ir.next();
        ir.stepInto();
        assertTrue(ir.hasNext());
        assertEquals(ir.next(), IonType.INT);
        assertEquals(ir.getFieldName(), "12");
        assertEquals(ir.getInt(), -12);
        assertTrue(ir.hasNext());
        assertEquals(ir.next(), IonType.FLOAT);
        assertEquals(ir.getFieldName(), "12242.124598129");
        assertEquals(ir.getDouble(), 12242.124598129);
        assertTrue(ir.hasNext());
        assertEquals(ir.next(), IonType.BOOL);
        assertEquals(ir.getFieldName(), "Almost Done.");
        assertEquals(ir.getBool(), true);
        assertTrue(ir.hasNext());
        assertEquals(ir.next(), IonType.BOOL);
        assertEquals(ir.getFieldName(), "This is a test String.");
        assertEquals(ir.getBool(), true);
        assertTrue(ir.hasNext());
        assertEquals(ir.next(), IonType.BOOL);
        assertEquals(ir.getFieldName(), "false");
        assertEquals(ir.getBool(), false);
        assertTrue(ir.hasNext());
        assertEquals(ir.next(), IonType.INT);
        assertEquals(ir.getFieldName(), "long");
        assertEquals(ir.getLong(), (long) 9326);    
        assertTrue(ir.hasNext());
        assertEquals(ir.next(), IonType.BOOL);
        assertEquals(ir.getFieldName(), "true");
        assertEquals(ir.getBool(), true);
        assertFalse(ir.hasNext());
        ir.stepOut();
        assertFalse(ir.hasNext());
    }
    
    @Test
    public void testChecker() throws DecodingException, EncodingException {
        HashMap m = new HashMap();
        m.put("String", "String");
        m.put("String \0", "\0");
        m.put("String 0", "");
        m.put("byte -1", (byte)-1);
        m.put("byte 1", (byte)1);
        m.put("byte 0", (byte)0);
        m.put("byte Max", (byte)Byte.MAX_VALUE);
        m.put("byte Min", (byte)Byte.MIN_VALUE);
        m.put("short -1", (short)-1);
        m.put("short 1", (short)1);
        m.put("short 0", (short)0);
        m.put("short Max", (short)Short.MAX_VALUE);
        m.put("short Min", (short)Short.MIN_VALUE);
        m.put("int -1", (int)-1);
        m.put("int 1", (int)1);
        m.put("int 0", (int)0);
        m.put("int Max", (int)Integer.MAX_VALUE);
        m.put("int Min", (int)Integer.MIN_VALUE);
        m.put("long -1", (long)-1);
        m.put("long 1", (long)1);
        m.put("long 0", (long)0);
        m.put("long Max", (long)Long.MAX_VALUE);
        m.put("long Min", (long)Long.MIN_VALUE);
        //m.put("BigInt", new BigInteger("-1"));
        //m.put("BigDecimal", new BigDecimal("-1"));
        m.put("float -1", (float)-1);
        m.put("float 1", (float)1);
        m.put("float 0", (float)0);
        m.put("float Max", (float)Float.MAX_VALUE);
        m.put("float Min", (float)Float.MIN_VALUE);
        m.put("float MinNorm", (float)0x1.0p-126f);
        m.put("float -inf", (float)Float.NEGATIVE_INFINITY);
        m.put("float inf", (float)Float.POSITIVE_INFINITY);
        m.put("float nan", (float)Float.NaN);
        m.put("double -1", (double)-1);
        m.put("double 1", (double)1);
        m.put("double 0", (double)0);
        m.put("double Max", (double)Double.MAX_VALUE);
        m.put("double Min", (double)Double.MIN_VALUE);
        m.put("double MinNorm", (double)0x1.0p-1022);
        m.put("double -inf", (double)Double.NEGATIVE_INFINITY);
        m.put("double inf", (double)Double.POSITIVE_INFINITY);
        m.put("double nan", (double)Double.NaN);
        m.put("Char", 'n');
        m.put("Date 0", new Date(0));
        m.put("Date -1", new Date(-1));
        m.put("Date 1", new Date(1));
        m.put("Date Max", new Date(Long.MAX_VALUE));
        m.put("Date Min", new Date(Long.MIN_VALUE));
        m.put("true", true);
        m.put("false", false);
        m.put("null", null);
        StringBuffer sbuffer = new StringBuffer(Short.MAX_VALUE+1);
        for (int i=0; i<Short.MAX_VALUE+1;i++) {
            sbuffer.append('\0');
        }
        m.put("String big", sbuffer.toString());
        m.put("char[] big", sbuffer.toString().toCharArray());
        m.put("char[] 3", "foo".toCharArray());
        m.put("char[] 0", "".toCharArray());
        m.put("byte[] big", new byte[Short.MAX_VALUE+1]);
        m.put("Object[] big", new Object[Short.MAX_VALUE+1]);
        m.put("short[] big", new short[Short.MAX_VALUE+1]);
        m.put("int[] big", new int[Short.MAX_VALUE+1]);
        m.put("long[] big", new long[Short.MAX_VALUE+1]);
        m.put("float[] big", new float[Short.MAX_VALUE+1]);
        m.put("double[] big", new double[Short.MAX_VALUE+1]);
        m.put("Date[] big", new Date[Short.MAX_VALUE+1]);
        m.put("String[] big", new String[Short.MAX_VALUE+1]);
        m.put("byte[] 0", new byte[]{});
        m.put("byte[] 1", new byte[]{(byte)0xFF,0x01,0x00});
        m.put("byte[] 2", new byte[]{0x00,0x01,(byte)0xFF});
        m.put("byte[] 3", new byte[]{0x01,(byte)0xFF,0x00});
        m.put("Object [] 0", new Object[]{});
        m.put("short []", new short[]{});
        m.put("int []", new int[]{});
        m.put("long []", new long[]{});
        m.put("float []", new float[]{});
        m.put("double []", new double[]{});
        m.put("boolean []", new boolean[]{});
        m.put("Date []", new Date[]{});
        m.put("String []", new String[]{});
        m.put("null []", new Object[]{null});
        m.put("Object []", m.values().toArray());
        m.put("keys",new ArrayList(m.keySet()));
        m.put("values", new Vector(m.values()));
        m.put("map", new TreeMap(m));
        byte[] bout = conv.serialize(m);
        Map out = conv.deserialize(bout);
        assertTrue(checker.areEqual(m, out));
        assertTrue(checker.areEqual(out, m));
        assertTrue(checker.areEqual(m, m));
        assertTrue(checker.areEqual(conv.deserialize(bout),out));
    }
    
    private Object get(IonIterator ir, SymbolTable syms, String name) {
        int sid = syms.findSymbol(name);
        IonIterator cursor = IonIterator.makeIterator(ir);
        while (cursor.hasNext()) {
            cursor.next();
            if (cursor.getFieldId() == sid) {
                return cursor.getString();
            }
        }
        return null;
    }
    
    @Test
    public void testBadMap () throws DecodingException {
        byte[] serialized = new byte[]{-84, -19, 0, 5, 119, 113, 0, 36, 57, 48, 55, 56, 50, 56, 99, 51, 45, 52, 98, 55, 101, 45, 52, 49, 102, 56, 45, 97, 100, 49, 97, 45, 99, 100, 57, 97, 52, 99, 56, 100, 51, 100, 98, 57, 0, 38, 116, 99, 112, 58, 47, 47, 104, 103, 97, 100, 103, 105, 108, 46, 100, 101, 115, 107, 116, 111, 112, 46, 97, 109, 97, 122, 111, 110, 46, 99, 111, 109, 58, 52, 54, 52, 51, 52, 0, 21, 83, 69, 65, 44, 85, 83, 49, 44, 48, 65, 51, 52, 48, 67, 48, 48, 44, 99, 111, 114, 112, 0, 0, 1, 24, 26, 60, 80, 24, 0, 0, 0, 1};
        try {
            Map m = conv.deserialize(serialized);
            assertTrue(false);
        } catch (DecodingException e) {
            assertTrue(true);
        }
    }
    
    @Test
    public void testRandom () throws EncodingException, DecodingException {
        Map<Map,Exception> results = new TreeMap();
        for (int i=0;i<1000;i++) {
            Random rand = new Random(i); 
            Map in = checker._mapSerializer.generateRandom(rand, 0);
            byte [] bout = conv.serialize(in);
            Map out = conv.deserialize(bout);
            assertTrue(checker.areEqual(in, out));
        }
    }
    
    @Test
    public void testList()
    {
        IonSystem sys = SystemFactory.newSystem();
        IonLoader loader = sys.newLoader();
        IonStruct s = sys.newNullStruct();
        for (int i = 0; i<5; i++) {
            IonList l = sys.newNullList();
            for (int j = 0;j<5;j++) {
                l.add(sys.newString("Hello: "+j));
            }
            s.add("HashKey "+i, l);
        }
        loader.load(sys.newDatagram(s).toBytes());
    }
}