package com.amazon.dse.messaging.datagram;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.amazon.ion.stateless.ContainerReaders;
import com.amazon.ion.stateless.StatelessReader;

/**
 * @author kaitchuc
 *  Does lazy deserialization of a map.
 */
class LazyMap extends AbstractMap<String, Object> implements Map<String,Object> {

    private static final ContainerReaders.structReader reader = new ContainerReaders.structReader();
    private final IonTypedCreator creator;
    private final Map<String,Integer> serilizedKeys; 
    private final HashMap<String,Object> realMap;
    private final ByteBuffer datagram;
    
    /**
     * Create a new lazy map with no starting content.
     */
    LazyMap(IonTypedCreator creator) {
        this.creator = creator;
        realMap = new HashMap<String,Object>();
        datagram = null;
        serilizedKeys = new HashMap<String,Integer>(0);
    }
    
    LazyMap(IonTypedCreator creator, ByteBuffer buff) throws IOException {
        this.creator = creator;
        datagram = buff.asReadOnlyBuffer();
        StatelessReader.skip(buff);
        reader.readStructStart(datagram);
        datagram.mark();
        realMap = new HashMap<String,Object>();
        serilizedKeys = new HashMap<String,Integer>();

        while (datagram.hasRemaining()) {
            serilizedKeys.put((String)creator.getOrigionalValue(datagram, null),datagram.position());
            StatelessReader.skip(datagram);
        }
    }
    
    @Override
    public void clear() {
        serilizedKeys.clear();
        realMap.clear();
    }

    @Override
    public Object put(String key, Object value) {
        Object result = get(key);
        realMap.put(key, value);
        return result;
    }


    @Override
    public int size() {
        int size = realMap.size();
        size += serilizedKeys.size();
        return size;
    }

    @Override
    public Collection<Object> values() {
        deserializeAll();
        return realMap.values();
    }

    @Override
    public boolean containsKey(Object key) {
        if (!realMap.containsKey(key) && !serilizedKeys.containsKey(key))
            return false;
        return true;
    }

    @Override
    public boolean containsValue(Object value) {
        deserializeAll();
        return realMap.containsValue(value);
    }

    @Override
    public Set<java.util.Map.Entry<String, Object>> entrySet() {
        deserializeAll();
        return realMap.entrySet();
    }

    @Override
    public Object get(Object key) {
        deserialize((String)key);
        return realMap.get(key);
    }
    
    private void deserialize(String key) {
        Integer pos = serilizedKeys.remove(key);
        if (pos == null)
            return;
        extractValue(key,pos);
    }

    private void extractValue(String key,int pos) {
        try {
            datagram.position(pos);
            Object value = creator.getOrigionalValue(datagram, null);
            realMap.put(key, value);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void deserializeAll() {
        for (Iterator<Map.Entry<String, Integer>> entrys = serilizedKeys.entrySet().iterator();entrys.hasNext();) {
            Map.Entry<String, Integer> entry = entrys.next();
            extractValue(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public boolean isEmpty() {
        return realMap.isEmpty() && serilizedKeys.isEmpty();
    }

    @Override
    public Set<String> keySet() {
        deserializeAll();
        return realMap.keySet();
    }

    @Override
    public void putAll(Map<? extends String, ? extends Object> m) {
        for (Map.Entry<? extends String, ? extends Object> e : m.entrySet()) {
            put(e.getKey(),e.getValue());
        }            
    }

    @Override
    public Object remove(Object key) {
        deserialize((String)key);
        return realMap.remove(key);
    }
    
}
