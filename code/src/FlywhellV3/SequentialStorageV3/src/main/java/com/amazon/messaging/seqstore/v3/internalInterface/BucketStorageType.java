package com.amazon.messaging.seqstore.v3.internalInterface;

import java.util.HashMap;
import java.util.Map;

public enum BucketStorageType {
    DedicatedDatabase(1, true),
    SharedDatabase(2, false),
    DedicatedDatabaseInLLMEnv(3, true);
    
    private static Map<Byte, BucketStorageType> idToBucketStorageMap;
    
    static {
        idToBucketStorageMap = new HashMap<Byte, BucketStorageType>();
        for( BucketStorageType val : values() ) {
            idToBucketStorageMap.put( val.getId(), val);
        }
    }
    
    public static BucketStorageType getStorageTypeForId(byte id) {
        return idToBucketStorageMap.get(id);
    }
    
    private BucketStorageType(int id, boolean dedicated) {
        this.id = (byte) id;
        this.dedicated = dedicated;
    }
    
    public byte getId() {
        return id;
    }
    
    public boolean isDedicated() {
        return dedicated;
    }
    
    @Override
    public String toString() {
        return name() + "(" + id + ")";
    }
    
    private byte id;
    private boolean dedicated;
}