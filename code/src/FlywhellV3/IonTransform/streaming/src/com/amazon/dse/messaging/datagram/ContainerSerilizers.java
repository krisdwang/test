package com.amazon.dse.messaging.datagram;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonWriter;

class ContainerSerilizers {

    static class NumberDeserializer extends AbstractSerializer<Number> {
        
        final IonTypedCreator creator;

        public NumberDeserializer(IonTypedCreator creator) {
            this.creator = creator;
        }
        
        @Override
        public Number getOrigionalValue(IonReader stream) throws IOException {
            String[] annotations = stream.getAnnotations();
            if (annotations == null || annotations.length <= 0 || annotations[0] == null)
                throw new IOException("Corrupt datagram.");
            AbstractSerializer s = creator.getDeserializer(annotations[0]);
            if (s == null)
                throw new IOException("Corrupt datagram.");
            return (Number) s.getOrigionalValue(stream);
        }

        @Override
        void writeValue(Number value, IonWriter writer) {
            throw new IllegalArgumentException("Method not supported.");
        }

        @Override
        Number generateRandomValue(Random rand, int length, int depth) {
            throw new IllegalArgumentException("Method not supported.");
        }
    }
    
    static class ListSerializer extends AbstractSerializer<Object> {
        static final String Array = "Array";

        private final IonTypedCreator creator;

        public ListSerializer(IonTypedCreator creator) {
            this.creator = creator;
        }
        
        public void writeValue(Object in, IonWriter writer) throws IOException {
            Collection<Object> c;
            if ((in instanceof Collection)) {
                c = (Collection<Object>) in;
            } else { // It must be an array of objects.
                writer.addAnnotation(Array);
                c = Arrays.asList((Object[]) in);
            }
            writer.startList();
            for (Object val : c) {
                creator.writeValue(val, writer);
            }
            writer.closeList();
        }

        @Override
        boolean areEqual(Object a, Object b) {
            Object[] aArray;
            Object[] bArray;
            if (a instanceof Collection) {
                if (!(b instanceof Collection))
                    return false;
                aArray = ((Collection) a).toArray();
                bArray = ((Collection) b).toArray();
            } else {
                aArray = (Object[]) a;
                bArray = (Object[]) b;
            }
            if (aArray.length != bArray.length)
                return false;
            for (int i = 0; i < aArray.length; i++) {
                if (!creator.areEqual(aArray[i], bArray[i]))
                    return false;
            }
            return true;
        }

        public Object getOrigionalValue(IonReader stream) throws IOException {
            Collection<Object> result = new ArrayList<Object>();
            String[] annotations = stream.getAnnotations();
            boolean array = false;
            if (annotations != null && annotations.length > 0 && annotations[0] != null) {
                if (annotations[0].equals(Array))
                    array = true;
                else {
                    AbstractSerializer serializer = creator.getDeserializer(annotations[0]);
                    if (serializer == null)
                        throw new IOException("Corrupt datagram.");
                    return serializer.getOrigionalValue(stream);
                }
            }

            stream.stepInto();
            while (stream.hasNext()) {
                result.add(creator.getOrigionalValue(stream));
            }
            stream.stepOut();

            if (array)
                return result.toArray();
            else
                return result;
        }

        @Override
        Object generateRandomValue(Random rand, int length, int depth) {
            boolean array = rand.nextBoolean();
            ArrayList<Object> result = new ArrayList<Object>(length);
            for (int i = 0; i < length; i++) {
                result.add(creator.generateRandom(rand, depth + 1));
            }
            return array ? result.toArray() : result;
        }
    }
    
    static abstract class MapSerializer<M extends Map<String, Object>> extends AbstractSerializer<M> {

        protected final IonTypedCreator creator;

        public MapSerializer(IonTypedCreator creator) {
            this.creator = creator;
        }

        public void writeValue(M map, IonWriter writer) throws IOException {
            writer.startStruct();
            for (Map.Entry<String, Object> pair : map.entrySet()) {
                writer.writeFieldname(pair.getKey());
                creator.writeValue(pair.getValue(), writer);
            }
            writer.closeStruct();
        }

        @Override
        boolean areEqual(M a, M b) {
            if (a.size() != b.size())
                return false;
            for (String key : a.keySet()) {
                if (!b.containsKey(key) || !creator.areEqual(a.get(key), b.get(key)))
                    return false;
            }
            return true;
        }
        
        M getOrigionalValue(IonReader stream, M map) throws IOException {
            stream.stepInto();
            while (stream.hasNext()) {
                map.put(stream.getFieldName(),creator.getOrigionalValue(stream));
            }
            stream.stepOut();
            return map;
        }
        
        M generateRandomValue(Random rand, int length, int depth, M map) {
            for (int i = 0; i < length; i++) {
                map.put("Depth:" + depth + ",Pos:" + String.format("%03d", i), creator.generateRandom(
                        rand, depth + 1));
            }
            return map;
        }
    }
    
    static class NormalMapSerializer extends MapSerializer<Map<String,Object>> {
        
        public NormalMapSerializer(IonTypedCreator creator) {
            super(creator);
        }
        @Override
        public Map<String,Object> getOrigionalValue(IonReader stream) throws IOException {
            return super.getOrigionalValue(stream, new HashMap());
        }
        @Override
        Map<String, Object> generateRandomValue(Random rand, int length, int depth) {
            return super.generateRandomValue(rand, length, depth, new TreeMap<String,Object>());
        }
    }
    
//    static class LazyMapSerializer extends MapSerializer<LazyMap> {
//        
//        public LazyMapSerializer(IonTypedCreator creator) {
//            super(creator);
//        }
//
//        @Override
//        LazyMap generateRandomValue(Random rand, int length, int depth) {
//            return super.generateRandomValue(rand, length, depth,new LazyMap(creator));
//        }
//
//        @Override
//        public LazyMap getOrigionalValue(IonReader stream) throws IOException {
//            return super.getOrigionalValue(stream,new LazyMap(creator));
//        }
//    }
}
