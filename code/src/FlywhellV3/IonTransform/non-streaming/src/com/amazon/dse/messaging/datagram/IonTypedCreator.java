package com.amazon.dse.messaging.datagram;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonList;
import com.amazon.ion.IonLoader;
import com.amazon.ion.IonNull;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.impl.IonBlobImpl;
import com.amazon.ion.impl.IonBoolImpl;
import com.amazon.ion.impl.IonFloatImpl;
import com.amazon.ion.impl.IonIntImpl;
import com.amazon.ion.impl.IonStringImpl;
import com.amazon.ion.impl.IonTimestampImpl;
import com.amazon.ion.system.SystemFactory;


public class IonTypedCreator extends AbstractSerializer<Object> {

    private static final IonSystem sys = SystemFactory.newSystem();

    private final Map<String, AbstractSerializer<?>> _serializerMap = new HashMap<String, AbstractSerializer<?>>();
    
    private final AbstractSerializer<Map<String,Object>> _mapSerializer = new AbstractSerializer<Map<String,Object>>(sys) {
        public IonValue getIonValue(Map<String,Object> map)
        {
            IonStruct result = sys.newNullStruct();
            for (Map.Entry<String,Object> pair : map.entrySet()) {
                result.add(pair.getKey(), IonTypedCreator.this.getIonValue(pair.getValue()));
            }
            return result;
        }

        public Map<String,Object> getOrigionalValue(IonValue orig)
        {
            IonStruct struct = (IonStruct) orig;
            Map<String,Object> result = new HashMap<String,Object>();
            struct.deepMaterialize();
            for (IonValue val : struct) {
                result.put(val.getFieldName(),IonTypedCreator.this.getOrigionalValue(val));
            }
            return result;
        }
    };
    
    private final AbstractSerializer<Object> _listSerializer = new AbstractSerializer<Object>(sys) {

        public IonValue getIonValue(Object in)
        {
            if (in instanceof Collection) {
                Collection<IonValue> list = new ArrayList<IonValue>();           
                for (Object val : (Collection)in) {
                    list.add(IonTypedCreator.this.getSerializer(val.getClass()).getIonValue(val));
                }     
                return sys.newList(list);
            } else { //It must be an array of objects.
                Object[] array = (Object[])in;
                IonList result = sys.newNullList();
                
                AbstractSerializer serializer = null; 
                for (Object val : (Object[])in) {
                    if (serializer == null) {
                        serializer = IonTypedCreator.this.getSerializer(val.getClass());
                        result.addTypeAnnotation(val.getClass().getCanonicalName());
                    }
                    result.add(serializer.getIonValue(val));
                } 
                return result;
            }
        }

        public Object getOrigionalValue(IonValue orig)
        {
            if (orig.getTypeAnnotations() != null && orig.getTypeAnnotations().length>0) {
                AbstractSerializer serializer = _serializerMap.get(orig.getTypeAnnotations()[0]);
                return serializer.getOrigionalValue(orig);
            } 
            
            IonList list = (IonList) orig;
            list.deepMaterialize();
            
            Collection result = new ArrayList();
            for (IonValue val : list) {
                result.add(IonTypedCreator.this.getSerializer(val.getClass()).getOrigionalValue(val));
            }              
            return result;      
        }
    };
    
    private final AbstractSerializer<Number> _numberDeserializer = new AbstractSerializer<Number>(sys) {

        public Number getOrigionalValue(IonValue orig)
        {
            String[] annotations = orig.getTypeAnnotations();
            if (annotations == null || annotations.length <= 0 ||  annotations[0] == null)
                throw new IllegalStateException("Corrupt datagram.");
            AbstractSerializer s = _serializerMap.get(annotations[0]);
            if (s == null)
                throw new IllegalStateException("Corrupt datagram.");
            return (Number) s.getOrigionalValue(orig);

        }

        @Override
        IonValue getIonValue(Number o) {
            throw new IllegalArgumentException("Method not supported.");
        }
    };
    
    public IonTypedCreator() {
        super(sys);
        AbstractSerializer s = _numberDeserializer;
        _serializerMap.put(IonIntImpl.class.getCanonicalName(),s);
        _serializerMap.put(IonFloatImpl.class.getCanonicalName(),s);
        
        s = new Serializers.IntSerializer(sys);
        _serializerMap.put(Integer.class.getCanonicalName(),s);
        _serializerMap.put(int.class.getCanonicalName(),s);
        s = new Serializers.IntSerializer.IntArrayWriter(sys,s);
        _serializerMap.put(int[].class.getCanonicalName(),s);

        s = new Serializers.LongSerializer(sys);
        _serializerMap.put(Long.class.getCanonicalName(),s);
        _serializerMap.put(long.class.getCanonicalName(),s);
        s = new Serializers.LongSerializer.LongArrayWriter(sys,s);
        _serializerMap.put(long[].class.getCanonicalName(),s);

        s = new Serializers.ShortSerializer(sys);
        _serializerMap.put(Short.class.getCanonicalName(),s);
        _serializerMap.put(short.class.getCanonicalName(),s);
        s = new Serializers.ShortSerializer.ShortArrayWriter(sys,s);
        _serializerMap.put(short[].class.getCanonicalName(),s);

        s = new Serializers.ByteSerializer(sys);
        _serializerMap.put(Byte.class.getCanonicalName(),s);
        _serializerMap.put(byte.class.getCanonicalName(),s);

        s = new Serializers.BooleanSerializer(sys);
        _serializerMap.put(Boolean.class.getCanonicalName(),s);
        _serializerMap.put(boolean.class.getCanonicalName(),s);
        _serializerMap.put(IonBoolImpl.class.getCanonicalName(),s);
        s = new Serializers.BooleanSerializer.BooleanArrayWriter(sys,s);
        _serializerMap.put(boolean[].class.getCanonicalName(),s);
        
        s = new Serializers.DoubleSerializer(sys);
        _serializerMap.put(Double.class.getCanonicalName(),s);
        _serializerMap.put(double.class.getCanonicalName(),s);
        s = new Serializers.DoubleSerializer.DoubleArrayWriter(sys,s);
        _serializerMap.put(double[].class.getCanonicalName(),s);
        
        s = new Serializers.FloatSerializer(sys);
        _serializerMap.put(Float.class.getCanonicalName(),s);
        _serializerMap.put(float.class.getCanonicalName(),s);        
        s = new Serializers.FloatSerializer.FloatArrayWriter(sys,s);
        _serializerMap.put(float[].class.getCanonicalName(),s);
        
        s = new Serializers.CharArraySerializer(sys);
        _serializerMap.put(char[].class.getCanonicalName(),s);
        
        s = new Serializers.StringSerializer(sys);
        _serializerMap.put(String.class.getCanonicalName(),s);
        _serializerMap.put(IonStringImpl.class.getCanonicalName(),s);
        
        s = new Serializers.DateSerializer(sys);
        _serializerMap.put(Date.class.getCanonicalName(),s);
        _serializerMap.put(IonTimestampImpl.class.getCanonicalName(),s);

        s = new Serializers.ByteArraySerializer(sys);
        _serializerMap.put(byte[].class.getCanonicalName(),s);
        _serializerMap.put(IonBlobImpl.class.getCanonicalName(),s);
    }
    
    AbstractSerializer getSerializer(Class cl) {
        AbstractSerializer serializer;

        serializer = _serializerMap.get(cl.getCanonicalName());
        if (serializer != null)
            return serializer;

        if (Map.class.isAssignableFrom(cl) || IonStruct.class.isAssignableFrom(cl))
            return _mapSerializer;
        else if (Collection.class.isAssignableFrom(cl) || cl.isArray() || IonList.class.isAssignableFrom(cl))
            return _listSerializer;
        else
            return getSerializer(cl.getSuperclass());
    }

    public IonValue getIonValue(Object o)
    {
        if (o == null)
           return sys.newNull();
        AbstractSerializer s = getSerializer(o.getClass());
        return s.getIonValue(o);
    }

    public Object getOrigionalValue(IonValue orig)
    {
        if (orig instanceof IonNull)
            return null;
        AbstractSerializer s = getSerializer(orig.getClass());
        return s.getOrigionalValue(orig);
    }
    
    public IonDatagram getIonDatagram(Object o) {
        return sys.newDatagram(getIonValue(o));
    }

    public Object getOrigionalValue(byte[] bytes) {
        IonLoader l = sys.newLoader();
        return getOrigionalValue(l.load(bytes).get(0));
    }
    
    public Object getOrigionalValue(String str) {
        IonLoader l = sys.newLoader();
        return getOrigionalValue(l.load(str).get(0));
    }

}
