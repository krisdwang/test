package com.amazon.dse.discovery;

import java.util.List;

/**
 * An abstract registry interface that provides the ability to register or delete entries in the registry

 * @author hgadgil
 * 
 */
public abstract class DirectoryServiceRegisterInterface {

    /**
     * Register the specified record with the specified key in the registry
     * 
     * @param key
     * @param record
     */
    public abstract void register(String key, RegistryEntryRecord record) throws RegisterException;

    /**
     * Delete the value corresponding to the specified key
     * 
     * @param key
     */
    public abstract void delete(String key, List<RegistryEntryRecord> records) throws DeleteException;
}
