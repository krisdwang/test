package com.amazon.dse.messaging.datagram;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.streaming.IonIterator;
import com.amazon.ion.IonWriter;


public class IonTypedCreator extends AbstractSerializer<Object> {

    final Map<String, AbstractSerializer<?>> _serializerMap = new HashMap<String, AbstractSerializer<?>>();
    final Map<String, AbstractSerializer<?>> _deserializerMap = new HashMap<String, AbstractSerializer<?>>();
    
    final AbstractSerializer<Map<String,Object>> _mapSerializer = new ContainerSerilizers.NormalMapSerializer(this);
    final AbstractSerializer<Object> _listSerializer = new ContainerSerilizers.ListSerializer(this);
    private final AbstractSerializer<Number> _numberDeserializer = new ContainerSerilizers.NumberDeserializer(this);
        
    public IonTypedCreator() {
        AbstractSerializer s = _mapSerializer;
        _deserializerMap.put(IonType.STRUCT.toString(),s);
        s = _listSerializer;
        _deserializerMap.put(IonType.LIST.toString(),s);
        
        s = _numberDeserializer;
        _deserializerMap.put(IonType.INT.toString(),s);
        _deserializerMap.put(IonType.FLOAT.toString(),s);
        
//        s = new ContainerSerilizers.LazyMapSerializer(this);
//        _serializerMap.put(LazyMap.class.getCanonicalName(), s);
        
        s = new Serializers.IntSerializer();
        _serializerMap.put(Integer.class.getCanonicalName(),s);
        _deserializerMap.put(int.class.getCanonicalName(),s);
        
        s = new Serializers.IntArrayWriter();
        _serializerMap.put(int[].class.getCanonicalName(),s);
        _deserializerMap.put(int[].class.getCanonicalName(),s);

        s = new Serializers.LongSerializer();
        _serializerMap.put(Long.class.getCanonicalName(),s);
        _deserializerMap.put(long.class.getCanonicalName(),s);
        
        s = new Serializers.LongArrayWriter();
        _serializerMap.put(long[].class.getCanonicalName(),s);
        _deserializerMap.put(long[].class.getCanonicalName(),s);

        s = new Serializers.ShortSerializer();
        _serializerMap.put(Short.class.getCanonicalName(),s);
        _deserializerMap.put(short.class.getCanonicalName(),s);
        
        s = new Serializers.ShortArrayWriter();
        _serializerMap.put(short[].class.getCanonicalName(),s);
        _deserializerMap.put(short[].class.getCanonicalName(),s);

        s = new Serializers.ByteSerializer();
        _serializerMap.put(Byte.class.getCanonicalName(),s);
        _deserializerMap.put(byte.class.getCanonicalName(),s);

        s = new Serializers.DoubleSerializer();
        _serializerMap.put(Double.class.getCanonicalName(),s);
        _deserializerMap.put(double.class.getCanonicalName(),s);
        
        s = new Serializers.DoubleArrayWriter();
        _serializerMap.put(double[].class.getCanonicalName(),s);
        _deserializerMap.put(double[].class.getCanonicalName(),s);
        
        s = new Serializers.FloatSerializer();
        _serializerMap.put(Float.class.getCanonicalName(),s);
        _deserializerMap.put(float.class.getCanonicalName(),s);    
        
        s = new Serializers.FloatArrayWriter();
        _serializerMap.put(float[].class.getCanonicalName(),s);
        _deserializerMap.put(float[].class.getCanonicalName(),s);
        
        s = new Serializers.BooleanSerializer();
        _serializerMap.put(Boolean.class.getCanonicalName(),s);
        _deserializerMap.put(IonType.BOOL.toString(),s);
        
        s = new Serializers.BooleanArrayWriter();
        _serializerMap.put(boolean[].class.getCanonicalName(),s);
        _deserializerMap.put(boolean[].class.getCanonicalName(),s);
        
        s = new Serializers.CharSerializer();
        _serializerMap.put(Character.class.getCanonicalName(),s);
        _deserializerMap.put(char.class.getCanonicalName(),s);
        
        s = new Serializers.CharArraySerializer();
        _serializerMap.put(char[].class.getCanonicalName(),s);
        //Deserialization handled by string
        
        s = new Serializers.StringSerializer();
        _serializerMap.put(String.class.getCanonicalName(),s);
        _deserializerMap.put(IonType.STRING.toString(),s);
  
        //Disabled until Ion supports arbitrary prisision decimals.
//        s = new Serializers.BigIntSerializer();
//        _serializerMap.put(BigInteger.class.getCanonicalName(),s);
        //Deserialization handled by BigDecimal
        
//        s = new Serializers.BigDecimalSerializer();
//        _serializerMap.put(BigDecimal.class.getCanonicalName(),s);
//        _deserializerMap.put(IonType.DECIMAL.toString(),s);
        
        s = new Serializers.DateSerializer();
        _serializerMap.put(Date.class.getCanonicalName(),s);
        _deserializerMap.put(IonType.TIMESTAMP.toString(),s);

        s = new Serializers.ByteArraySerializer();
        _serializerMap.put(byte[].class.getCanonicalName(),s);
        _deserializerMap.put(IonType.BLOB.toString(),s);
    }
    
    AbstractSerializer getSerializer(Class cl) {
        AbstractSerializer result = _getSerializer(cl);
        if (result==null)
            throw new IllegalArgumentException("Attempt to serialize an unsupported type: "+ cl.toString());
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
        else if (Collection.class.isAssignableFrom(cl) || cl.isArray())
            return _listSerializer;
        else {
            Class supr = cl.getSuperclass();
            return _getSerializer(supr);
        }
    }

    public void writeValue(Object value, IonWriter writer) throws IOException
    {
        if (value == null) {
           writer.writeNull();
           return;
        }        
        AbstractSerializer s = getSerializer(value.getClass());
        s.writeValue(value, writer);
    }
    
    @Override
    public boolean areEqual(Object a, Object b) {
        if (a == null)
            return b==null;
        AbstractSerializer s = getSerializer(a.getClass());
        return s.areEqual(a, b);
    }

    public Object getOrigionalValue(IonReader stream) throws IOException
    {
        IonType type = stream.next();
        if (type == IonType.NULL)
            return null;
        AbstractSerializer<?> s = getDeserializer(type.toString());
        return s.getOrigionalValue(stream);
    }

    AbstractSerializer<?> getDeserializer(String type) {
        return _deserializerMap.get(type);
    }

    @Override
    Object generateRandomValue(Random rand, int length, int depth) {
        boolean allowNested = rand.nextFloat()<(1.0/depth)?true:false;
        AbstractSerializer<?> s = getRandomSerializer(rand, allowNested);
        return s.generateRandom(rand, depth);
    }

    private AbstractSerializer<?> getRandomSerializer(Random rand, boolean allowNested) {
        int size = _serializerMap.size();
        if (allowNested)
            size += 2;
        int entry = rand.nextInt(size);
        if (entry == _serializerMap.size())
            return _listSerializer;
        else if (entry == _serializerMap.size()+1)
            return _mapSerializer;
        else {
            Iterator<AbstractSerializer<?>> iter = _serializerMap.values().iterator();
            for (int i=0;i<entry;i++) {
                iter.next();
            }
            return iter.next();
        }
    }
    
    
}
