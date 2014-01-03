package com.amazon.dse.messaging.datagram;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import com.amazon.dse.messaging.datagram.ContainerSerilizers.LazyMapSerializer;
import com.amazon.ion.IonException;
import com.amazon.ion.IonType;
import com.amazon.ion.stateless.*;

public final class IonTypedCreator extends AbstractSerializer<Object> {

    final SymbolTable _symbolTable_v1 = new SymbolTable();
    final Map<String, AbstractSerializer<?>> _serializerMap = new HashMap<String, AbstractSerializer<?>>();

    final Map<String, AbstractSerializer<?>> _deserializerMap = new HashMap<String, AbstractSerializer<?>>();

    static final ContainerReaders.structReader reader = new ContainerReaders.structReader();

    static final ValueWriters.nullWriter nullWriter = new ValueWriters.nullWriter();
    static final ValueReaders.nullReader nullReader = new ValueReaders.nullReader();

    final LazyMapSerializer _mapSerializer = new ContainerSerilizers.LazyMapSerializer(this);

    final AbstractSerializer<Collection> _listSerializer = new ContainerSerilizers.ListSerializer(this);

    final AbstractSerializer<Object[]> _arraySerializer = new ContainerSerilizers.ArraySerializer(this);

    public IonTypedCreator() {
        AbstractSerializer s = _mapSerializer;
        _deserializerMap.put(IonType.STRUCT.toString(), s);
        s = _listSerializer;
        _deserializerMap.put(_symbolTable_v1.add("list").toString()/*11*/, s);
        s = _arraySerializer;
        _deserializerMap.put(_symbolTable_v1.add("array").toString()/*12*/, s);

        s = new ContainerSerilizers.LazyMapSerializer(this);
        _serializerMap.put(LazyMap.class.getCanonicalName(), s);

        s = new Serializers.BooleanSerializer();
        _serializerMap.put(Boolean.class.getCanonicalName(), s);
        _deserializerMap.put(IonType.BOOL.toString(), s);

        s = new Serializers.BooleanArrayWriter();
        _serializerMap.put(boolean[].class.getCanonicalName(), s);
        _deserializerMap.put(String.valueOf(_symbolTable_v1.add(boolean[].class.getCanonicalName()))/*13*/, s);
        
        s = new Serializers.ByteSerializer();
        _serializerMap.put(Byte.class.getCanonicalName(), s);
        _deserializerMap.put(String.valueOf(_symbolTable_v1.add(byte.class.getCanonicalName()))/*14*/, s);
        _deserializerMap.put(String.valueOf(_symbolTable_v1.add("U"+byte.class.getCanonicalName()))/*15*/, s);
        
        s = new Serializers.ShortSerializer();
        _serializerMap.put(Short.class.getCanonicalName(), s);
        _deserializerMap.put(String.valueOf(_symbolTable_v1.add(short.class.getCanonicalName()))/*16*/, s);
        _deserializerMap.put(String.valueOf(_symbolTable_v1.add("U"+short.class.getCanonicalName()))/*17*/, s);

        s = new Serializers.ShortArrayWriter();
        _serializerMap.put(short[].class.getCanonicalName(), s);
        _deserializerMap.put(String.valueOf(_symbolTable_v1.add(short[].class.getCanonicalName()))/*18*/, s);
        _deserializerMap.put(String.valueOf(_symbolTable_v1.add("U"+short[].class.getCanonicalName()))/*19*/, s);
        
        s = new Serializers.IntSerializer();
        _serializerMap.put(Integer.class.getCanonicalName(), s);
        _deserializerMap.put(String.valueOf(_symbolTable_v1.add(int.class.getCanonicalName()))/*20*/, s);
        _deserializerMap.put(String.valueOf(_symbolTable_v1.add("U"+int.class.getCanonicalName()))/*21*/, s);

        s = new Serializers.IntArrayWriter();
        _serializerMap.put(int[].class.getCanonicalName(), s);
        _deserializerMap.put(String.valueOf(_symbolTable_v1.add(int[].class.getCanonicalName()))/*22*/, s);
        _deserializerMap.put(String.valueOf(_symbolTable_v1.add("U"+int[].class.getCanonicalName()))/*23*/, s);

        s = new Serializers.LongSerializer();
        _serializerMap.put(Long.class.getCanonicalName(), s);
        _deserializerMap.put(String.valueOf(_symbolTable_v1.add(long.class.getCanonicalName()))/*24*/, s);
        _deserializerMap.put(String.valueOf(_symbolTable_v1.add("U"+long.class.getCanonicalName()))/*25*/, s);

        s = new Serializers.LongArrayWriter();
        _serializerMap.put(long[].class.getCanonicalName(), s);
        _deserializerMap.put(String.valueOf(_symbolTable_v1.add(long[].class.getCanonicalName()))/*26*/, s);
        _deserializerMap.put(String.valueOf(_symbolTable_v1.add("U"+long[].class.getCanonicalName()))/*27*/, s);

        s = new Serializers.FloatSerializer();
        _serializerMap.put(Float.class.getCanonicalName(), s);
        _deserializerMap.put(String.valueOf(_symbolTable_v1.add(float.class.getCanonicalName()))/*28*/, s);
        _deserializerMap.put(String.valueOf(_symbolTable_v1.add("U"+float.class.getCanonicalName()))/*29*/, s);

        s = new Serializers.FloatArrayWriter();
        _serializerMap.put(float[].class.getCanonicalName(), s);
        _deserializerMap.put(String.valueOf(_symbolTable_v1.add(float[].class.getCanonicalName()))/*30*/, s);
        _deserializerMap.put(String.valueOf(_symbolTable_v1.add("U"+float[].class.getCanonicalName()))/*31*/, s);
        
        s = new Serializers.DoubleSerializer();
        _serializerMap.put(Double.class.getCanonicalName(), s);
        _deserializerMap.put(String.valueOf(_symbolTable_v1.add(double.class.getCanonicalName()))/*32*/, s);
        _deserializerMap.put(String.valueOf(_symbolTable_v1.add("U"+double.class.getCanonicalName()))/*33*/, s);

        s = new Serializers.DoubleArrayWriter();
        _serializerMap.put(double[].class.getCanonicalName(), s);
        _deserializerMap.put(String.valueOf(_symbolTable_v1.add(double[].class.getCanonicalName()))/*34*/, s);
        _deserializerMap.put(String.valueOf(_symbolTable_v1.add("U"+double[].class.getCanonicalName()))/*35*/, s);

        s = new Serializers.CharSerializer();
        _serializerMap.put(Character.class.getCanonicalName(), s);
        _deserializerMap.put(String.valueOf(_symbolTable_v1.add(char.class.getCanonicalName()))/*36*/, s);

        s = new Serializers.CharArraySerializer();
        _serializerMap.put(char[].class.getCanonicalName(), s);
        _deserializerMap.put(String.valueOf(_symbolTable_v1.add(char[].class.getCanonicalName()))/*37*/, s);

        s = new Serializers.StringSerializer();
        _serializerMap.put(String.class.getCanonicalName(), s);
        _deserializerMap.put(IonType.STRING.toString(), s);

        // Disabled until Ion supports arbitrary prisision decimals.
        // s = new Serializers.BigIntSerializer();
        // _serializerMap.put(BigInteger.class.getCanonicalName(),s);
        // Deserialization handled by BigDecimal

        // s = new Serializers.BigDecimalSerializer();
        // _serializerMap.put(BigDecimal.class.getCanonicalName(),s);
        // _deserializerMap.put(IonType.DECIMAL.toString(),s);

        s = new Serializers.DateSerializer();
        _serializerMap.put(Date.class.getCanonicalName(), s);
        _deserializerMap.put(IonType.TIMESTAMP.toString(), s);

        s = new Serializers.ByteArraySerializer();
        _serializerMap.put(byte[].class.getCanonicalName(), s);
        _deserializerMap.put(IonType.BLOB.toString(), s);
    }

    AbstractSerializer getSerializer(Class cl) {
        AbstractSerializer result = _getSerializer(cl);
        if (result == null)
            throw new IllegalArgumentException("Attempt to serialize an unsupported type: " + cl.toString());
        else
            return result;
    }

    AbstractSerializer _getSerializer(Class cl) {
        AbstractSerializer serializer;
        if (cl == null)
            return null;

        serializer = _serializerMap.get(cl.getCanonicalName());
        if (serializer != null)
            return serializer;

        if (Map.class.isAssignableFrom(cl))
            return _mapSerializer;
        else if (Collection.class.isAssignableFrom(cl))
            return _listSerializer;
        else if (cl.isArray())
            return _arraySerializer;
        else {
            Class supr = cl.getSuperclass();
            return _getSerializer(supr);
        }
    }

    public void writeValue(Object value, ByteArrayOutputStream out, StatefullEncoder enc) throws IOException {
        if (value == null) {
            nullWriter.writeValue(null, value, out);
        } else {
            AbstractSerializer s = getSerializer(value.getClass());
            s.writeValue(value, out, null);
        }
    }

    @Override
    public boolean areEqual(Object a, Object b) {
        if (a == null)
            return b == null;
        AbstractSerializer s = getSerializer(a.getClass());
        return s.areEqual(a, b);
    }

    public Object getOrigionalValue(ByteBuffer stream, StatefullDecoder dec) throws IOException {
        int[] readAnnotations = reader.readAnnotations(stream);
        AbstractSerializer<?> s;
        if (readAnnotations != null ) {
            if (readAnnotations.length != 1)
                throw new IonException("Too many annotations.");
            s = getDeserializer(String.valueOf(readAnnotations[0]));
            if (s == null)
                throw new IonException("Unexpected type: "+ readAnnotations[0]);
        }
        else {
            IonType type = reader.readType(stream);
            if (type == IonType.NULL) {
                nullReader.readValue(stream);
                return null;
            }
            s = getDeserializer(type.name());
            if (s == null)
                throw new IonException("Unexpected type: "+ type.name());
        }
        return s.getOrigionalValue(stream, null);
    }

    AbstractSerializer<?> getDeserializer(String type) {
        return _deserializerMap.get(type);
    }

    @Override
    Object generateRandomValue(Random rand, int length, int depth) {
        float randValue = rand.nextFloat();
        double prob = (1.0 /depth);
        AbstractSerializer<?> s = getRandomSerializer(rand, randValue < prob);
        return s.generateRandom(rand, depth);
    }

    private AbstractSerializer<?> getRandomSerializer(Random rand, boolean allowNested) {
        int size = _serializerMap.size();
        if (allowNested)
            size += 3;
        int entry = rand.nextInt(size);
        if (entry == _serializerMap.size())
            return _listSerializer;
        else if (entry == _serializerMap.size() + 1)
            return _arraySerializer;
        else if (entry == _serializerMap.size() + 2)
            return _mapSerializer;
        else {
            Iterator<AbstractSerializer<?>> iter = _serializerMap.values().iterator();
            for (int i = 0; i < entry; i++) {
                iter.next();
            }
            return iter.next();
        }
    }

}
