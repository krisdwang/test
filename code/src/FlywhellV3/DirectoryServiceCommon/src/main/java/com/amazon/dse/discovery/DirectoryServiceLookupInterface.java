package com.amazon.dse.discovery;

import java.util.List;

/**
 * An abstract registry interface that provides the ability to query a registry

 * @author hgadgil
 * 
 */
public abstract class DirectoryServiceLookupInterface {

    /**
     * Lookup and return the registry records associated with the specified key
     * 
     * @param key
     *            - The key to lookup
     * @return A list of Registry entry records (possibly empty if no values are
     *         found or the key does not exist)
     */
    public abstract List<RegistryEntryRecord> lookup(String key) throws LookupException;
}
