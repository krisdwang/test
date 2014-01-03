package com.amazon.messaging.impls;

import java.util.Map;

import com.amazon.messaging.interfaces.Tagger;

public class MapTagger<Type, State> implements Tagger<Map<String, Object>, Type, String, State> {

    private final static String TYPE = "MapTagger_ClassType";

    @Override
    public Map<String, Object> addTag(Type orig, Map<String, Object> transformed, State state) {
        transformed.put(TYPE, orig.getClass().getName());
        return transformed;
    }

    @Override
    public String getTag(Map<String, Object> transformed, State state) {
        return (String) transformed.get(TYPE);
    }

}
