package com.amazon.dse.messaging.datagram;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import com.amazon.ion.LocalSymbolTable;
import com.amazon.ion.impl.LocalSymbolTableImpl;
import com.amazon.ion.impl.SystemSymbolTableImpl;
import com.amazon.ion.stateless.ContainerReaders;
import com.amazon.ion.stateless.ContainerWriters;
import com.amazon.ion.system.SystemFactory;

class ContainerSerilizers {
    
    static class ListSerializer extends AbstractSerializer<Collection> {
        static final ContainerWriters.listWriter writer = new ContainerWriters.listWriter();
        static final ContainerReaders.listReader reader = new ContainerReaders.listReader();
        static final int[] Annotations = new int[]{11};

        private final IonTypedCreator creator;

        public ListSerializer(IonTypedCreator creator) {
            this.creator = creator;
        }
        
        public void writeValue(Collection in, ByteArrayOutputStream out, StatefullEncoder enc) throws IOException {
            Collection<Object> c = (Collection<Object>) in;           
            ByteArrayOutputStream list = new ByteArrayOutputStream();
            for (Object val : c) {
                creator.writeValue(val, list, null);
            }
            writer.writeListStart(Annotations, list.size(), out);
            list.writeTo(out);
            writer.writeListEnd(out);
        }

        @Override
        boolean areEqual(Collection a, Collection b) {
            if (a.size() != b.size())
                return false;
            Object[] aArray = a.toArray();
            Object[] bArray = b.toArray();
            for (int i = 0; i < aArray.length; i++) {
                if (!creator.areEqual(aArray[i], bArray[i]))
                    return false;
            }
            return true;
        }

        public Collection getOrigionalValue(ByteBuffer buff, StatefullDecoder dec) throws IOException {
            int end = buff.limit();
            int length = reader.readListStart(buff);
            Collection<Object> result = new ArrayList<Object>(length);
            for (int i = 0; i < length; i++) {
                result.add(creator.getOrigionalValue(buff, null));
            }
            reader.readListEnd(end, buff);
            return result;
        }

        @Override
        Collection generateRandomValue(Random rand, int length, int depth) {
            ArrayList<Object> result = new ArrayList<Object>(length);
            for (int i = 0; i < length; i++) {
                result.add(creator.generateRandom(rand, depth + 1));
            }
            return result;
        }
    }
    

    static class ArraySerializer extends AbstractSerializer<Object[]> {
        static final ContainerWriters.listWriter writer = new ContainerWriters.listWriter();
        static final ContainerReaders.listReader reader = new ContainerReaders.listReader();
        static final int[] annotations = new int[]{12};

        private final IonTypedCreator creator;

        public ArraySerializer(IonTypedCreator creator) {
            this.creator = creator;
        }
        
        public void writeValue(Object[] in, ByteArrayOutputStream out, StatefullEncoder enc) throws IOException {
            Collection<Object> c = Arrays.asList(in);
            ByteArrayOutputStream list = new ByteArrayOutputStream();
            for (Object val : c) {
                creator.writeValue(val, list, null);
            }
            writer.writeListStart(annotations, list.size(), out);
            list.writeTo(out);
            writer.writeListEnd(out);
        }

        @Override
        boolean areEqual(Object[] a, Object[] b) {
            if (a.length != b.length)
                return false;
            for (int i = 0; i < a.length; i++) {
                if (!creator.areEqual(a[i], b[i]))
                    return false;
            }
            return true;
        }

        public Object[] getOrigionalValue(ByteBuffer buff, StatefullDecoder dec) throws IOException {
            int end = buff.limit();
            int length = reader.readListStart(buff);
            Object[] result = new Object[length];
            for (int i = 0; i < length; i++) {
                result[i] = creator.getOrigionalValue(buff, null);
            }
            reader.readListEnd(end, buff);
            return result;
        }

        @Override
        Object[] generateRandomValue(Random rand, int length, int depth) {
            ArrayList<Object> result = new ArrayList<Object>(length);
            for (int i = 0; i < length; i++) {
                result.add(creator.generateRandom(rand, depth + 1));
            }
            return result.toArray();
        }
    }
    
    static abstract class MapSerializer<M extends Map<String, Object>> extends AbstractSerializer<M> {

        protected final IonTypedCreator creator;
        static final ContainerWriters.structWriter writer = new ContainerWriters.structWriter();
        static final ContainerReaders.structReader reader = new ContainerReaders.structReader();
        
        public MapSerializer(IonTypedCreator creator) {
            this.creator = creator;
        }
        
        public void writeValue(M in, ByteArrayOutputStream out, StatefullEncoder enc) throws IOException {
            ByteArrayOutputStream map = new ByteArrayOutputStream();
            for (Map.Entry<String, Object> pair : in.entrySet()) {
                creator.writeValue(pair.getKey(), map, null);
                creator.writeValue(pair.getValue(), map, null);
            }
            writer.writeStructStart(null, null, map.size(), out);
            map.writeTo(out);
            writer.writeStructEnd(out);
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
        
        M getOrigionalValue(ByteBuffer in, M map) throws IOException {
            int limit = in.limit();
            reader.readStructStart(in);
            while (in.hasRemaining()) {
                map.put((String) creator.getOrigionalValue(in, null),creator.getOrigionalValue(in, null));
            }
            reader.readStructEnd(limit, in);
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
    
    static class SymbolTableSerializer extends AbstractSerializer<SymbolTable> {

        protected final IonTypedCreator creator;
        
        public SymbolTableSerializer(IonTypedCreator creator) {
            this.creator = creator;
        }
        
        @Override
        SymbolTable generateRandomValue(Random rand, int length, int depth) {
            throw new UnsupportedOperationException("Unimplemented");
        }

        @Override
        SymbolTable getOrigionalValue(ByteBuffer stream, StatefullDecoder dec) throws IOException {
            Map<String, Object> origionalValue = creator._mapSerializer.getOrigionalValue(stream, dec);
            return new SymbolTable(origionalValue);
        }

        @Override
        void writeValue(SymbolTable value, ByteArrayOutputStream writer, StatefullEncoder enc) throws IOException {
            creator.writeValue(value.getWireFormat(), writer, null);
        }
        
    }
    
    static class NormalMapSerializer extends MapSerializer<Map<String,Object>> {
        
        public NormalMapSerializer(IonTypedCreator creator) {
            super(creator);
        }
        @Override
        public Map<String,Object> getOrigionalValue(ByteBuffer in, StatefullDecoder dec) throws IOException {
            return super.getOrigionalValue(in, new LazyMap(creator));
            //return new LazyMap(creator,in);
        }
        @Override
        Map<String, Object> generateRandomValue(Random rand, int length, int depth) {
            return super.generateRandomValue(rand, length, depth, new TreeMap<String,Object>());
        }
    }
    
    static class LazyMapSerializer extends MapSerializer<LazyMap> {
        
        public LazyMapSerializer(IonTypedCreator creator) {
            super(creator);
        }

        @Override
        LazyMap generateRandomValue(Random rand, int length, int depth) {
            return super.generateRandomValue(rand, length, depth,new LazyMap(creator));
        }

        @Override
        public LazyMap getOrigionalValue(ByteBuffer in, StatefullDecoder dec) throws IOException {
            //return super.getOrigionalValue(in, new LazyMap(creator));
            return new LazyMap(creator,in);
        }
    }
}
