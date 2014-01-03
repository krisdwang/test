package com.amazon.dse.messaging.datagram;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

class SymbolTable implements Map<Integer, String> {
    
    String name = null;
    Integer version = null;
    int MaxID = 10;
    List<Map<String,Object>> imports = new ArrayList<Map<String,Object>>();
    Map<Integer, String> IdToVal = new HashMap<Integer, String>();
    Map<String, Integer> ValToID = new HashMap<String, Integer>();
    
    SymbolTable(Map<String,Object> wireform) {
        name = (String) wireform.get("name");
        version = (Integer) wireform.get("version");
        MaxID = (Integer) wireform.get("max_id");
        imports = (List<Map<String, Object>>) wireform.get("imports");
        Object symbols = wireform.get("symbols");
        if (symbols != null) {
            if (symbols instanceof Map) {
                for (Map.Entry<Integer, String> symbol : ((Map<Integer, String>) symbols).entrySet() )
                    this.put(symbol.getKey(), symbol.getValue());
            } else {
                for (String symbol : (Collection<String>) symbols)
                    this.add(symbol);
            }
        }
    }
    
    public SymbolTable() {
        //Using setters to populate the table...
    }

    public Map<String,Object> getWireFormat() {
        Map <String,Object> result = new HashMap<String, Object>();
        if (name!=null)
            result.put("name", name);
        if (version!=null)
            result.put("version", version);
        result.put("max_id", version);
        result.put("imports",imports);
        result.put("symbols", IdToVal);
        return result;
    }
    
    public Integer add(String symbol) {
        MaxID++;
        IdToVal.put(MaxID, symbol);
        ValToID.put(symbol, MaxID);
        return MaxID;
    }
    
    public void addImport(SymbolTable toImport) {
        if (!this.isEmpty()) throw new IllegalArgumentException("Cannot import a value to a populated SymbolTable.");
        Map<String,Object> imp = toImport.getWireFormat();
        imp.remove("symbols");
        imports.add(imp);
        MaxID += toImport.size();
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public void setVersion(Integer version) {
        this.version = version;
    }
    
    public Integer getValue(String symbol) {
        return ValToID.get(symbol);
    }
    
    @Override
    public void clear() {
        IdToVal.clear();
        ValToID.clear();
    }

    @Override
    public boolean isEmpty() {
        return IdToVal.isEmpty();
    }

    @Override
    public int size() {
        return IdToVal.size();
    }

    @Override
    public boolean containsKey(Object key) {
        return IdToVal.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return ValToID.containsKey(value);
    }

    @Override
    public Set<java.util.Map.Entry<Integer, String>> entrySet() {
        return IdToVal.entrySet();
    }

    @Override
    public String get(Object key) {
        return IdToVal.get(key);
    }

    @Override
    public Set<Integer> keySet() {
        return IdToVal.keySet();
    }

    @Override
    public String put(Integer key, String value) {
        if (key == null || key < 10)
            throw new IllegalArgumentException("Can only contain values > 10");
        MaxID = Math.max(key, MaxID);
        ValToID.put(value, key);
        return IdToVal.put(key, value);
    }

    @Override
    public void putAll(Map<? extends Integer, ? extends String> m) {
        for (Map.Entry<? extends Integer, ? extends String> e : m.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    @Override
    public String remove(Object key) {
        String result =  IdToVal.remove(key);
        ValToID.remove(result);
        return result;
    }

    @Override
    public Collection<String> values() {
        return IdToVal.values();
    }

}
