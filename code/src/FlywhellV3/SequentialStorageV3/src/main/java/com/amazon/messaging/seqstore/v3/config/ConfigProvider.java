package com.amazon.messaging.seqstore.v3.config;

import java.util.Set;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * This interface is used to retrieve the config to use for Flywheel/SequentialStorage stores and readers. Users 
 * must provide their own implementation that retrieves the configuration from whatever storage system
 * they use. 
 * <p>
 * <b>Important:</b> Config should always be available for all stores and readers. If config is missing for the
 * store or any of its readers the store cannot be loaded.  
 *
 */
public interface ConfigProvider<InfoType> {

    /**
     * Get the configuration for a store.
     * 
     * @param storeName the name of the store.
     * @return the configuration for the store. Must not be null.
     */
    @NonNull
    SeqStoreImmutableConfig getStoreConfig(String storeName) throws ConfigUnavailableException;
    
    /**
     * Get the set of required readers for the store. This is the minimal set of readers
     * that must be created when the store is loaded. If there are more readers already on
     * the store the will not be affected.
     *  
     * @param storeName the name of the store
     * @return the set of required readers
     * @throws ConfigUnavailableException if there is no configuration for the store.
     */
    @NonNull
    Set<String> getRequiredReaders(String storeName) throws ConfigUnavailableException;

    /**
     * Get the configuration for a reader. 
     * 
     * @param storeName the name of the store for the reader 
     * @param readerName the name of the reader.
     * @return the configuration for the reader. Must not be null.
     */
    @NonNull
    SeqStoreReaderImmutableConfig<InfoType> getReaderConfig(String storeName, String readerName)
        throws ConfigUnavailableException;

}
