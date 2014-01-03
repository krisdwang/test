package com.amazon.dse.messaging.datagram;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.amazon.ion.IonValue;
import com.amazon.ion.streaming.IonIterator;

/**
 * @author kaitchuc
 *  Does lazy deserialization of a map.
 */
class LazyMap implements Map<String,Object> {

    private final IonTypedCreator creator;
    private final ArrayList<String> serilizedKeys; 
    private final HashMap<String,Object> realMap;
    private final IonValue ionObject; 
    
    /**
     * Create a new lazy map with no starting content.
     */
    LazyMap(IonTypedCreator creator) {
        this.creator = creator;
        realMap = new HashMap<String,Object>();
        begining = null;
        serilizedKeys = new ArrayList<String>(0);
    }
    
    LazyMap(IonTypedCreator creator, IonIterator iter) {
        this.creator = creator;
        begining = IonIterator.makeIterator(iter);
        int size = begining.getContainerSize();
        realMap = new HashMap<String,Object>(size);
        serilizedKeys = new ArrayList<String>(size);
        begining.stepInto();

        IonIterator local = getIter();
        for (Integer i = 0;local.hasNext();i++) {
            serilizedKeys.add(iter.getFieldName());
        }
    }

    
    private IonIterator getIter() {
        //return begining;
        return IonIterator.makeIterator(begining);
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
        for (String key : serilizedKeys) 
            if (key != null)
                size++;
        return size;
    }

    @Override
    public Collection<Object> values() {
        deserializeAll();
        return realMap.values();
    }

    @Override
    public boolean containsKey(Object key) {
        if (!realMap.containsKey(key) && !serilizedKeys.contains(key))
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
        int pos = serilizedKeys.indexOf(key);
        if (pos == -1)
            return;
        IonIterator iter = getIter();
        for (int i = 0; i<pos; i++) {
            iter.next();
        }
        extractValue(key, pos, iter);
    }


    private void extractValue(String key, int pos, IonIterator iter) {
        try {
            realMap.put(key, creator.getOrigionalValue(iter));
            serilizedKeys.set(pos, null);
        } catch (IOException e) {
            throw new IllegalStateException("Datagram corrupt.",e);
        }
    }


    private void deserializeAll() {
        if (serilizedKeys.size() <= 0)
            return;
        IonIterator iter = getIter();
        for (int i = 0; i < serilizedKeys.size(); i++) {
            String key = serilizedKeys.get(i);
            iter.next();
            if (key == null)
                continue;
            extractValue(key, i, iter);
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
