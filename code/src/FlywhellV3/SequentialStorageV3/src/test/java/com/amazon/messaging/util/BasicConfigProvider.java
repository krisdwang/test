package com.amazon.messaging.util;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.amazon.messaging.seqstore.v3.config.ConfigProvider;
import com.amazon.messaging.seqstore.v3.config.ConfigUnavailableException;
import com.amazon.messaging.seqstore.v3.config.SeqStoreConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStoreImmutableConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStoreReaderConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStoreReaderImmutableConfig;
import com.amazon.messaging.seqstore.v3.exceptions.InvalidConfigException;

/**
 * Always returns a configuration.
 * 
 * @author robburke
 */
public class BasicConfigProvider<InfoType> implements ConfigProvider<InfoType> {
    private static final String SEPERATOR = ":";
    
    private final SeqStoreImmutableConfig defaultStoreConfig;
    private final SeqStoreReaderImmutableConfig<InfoType> defaultReaderConfig;

    private final ConcurrentHashMap<String, SeqStoreImmutableConfig> storeMap = new ConcurrentHashMap<String, SeqStoreImmutableConfig>();

    private final ConcurrentHashMap<String, SeqStoreReaderImmutableConfig<InfoType>> readerMap =
    	new ConcurrentHashMap<String, SeqStoreReaderImmutableConfig<InfoType>>();
    
    private final ConcurrentHashMap<String, Set<String>> requiredReaders 
        = new ConcurrentHashMap<String, Set<String>>();
    
    public BasicConfigProvider(
    		SeqStoreImmutableConfig defaultStoreConfig,
			SeqStoreReaderImmutableConfig<InfoType> defaultReaderConfig) {
		this.defaultStoreConfig = defaultStoreConfig;
		this.defaultReaderConfig = defaultReaderConfig;
	}

	@Override
    public SeqStoreImmutableConfig getStoreConfig(String storeName)
	    throws ConfigUnavailableException
	{
        storeMap.putIfAbsent(storeName, defaultStoreConfig);
        return storeMap.get(storeName);
    }
	
    @Override
    public SeqStoreReaderImmutableConfig<InfoType> getReaderConfig(String storeName, String readerName) 
        throws ConfigUnavailableException
    {
        readerMap.putIfAbsent(storeName + SEPERATOR + readerName, defaultReaderConfig);
        return readerMap.get(storeName + SEPERATOR + readerName);
    }
    
    @Override
    public Set<String> getRequiredReaders(String storeName) throws ConfigUnavailableException {
        Set<String> result = requiredReaders.get( storeName );
        if( result == null ) {
            return Collections.emptySet();
        }
        return result;
    }
    
    public void setRequiredReaders(String storeName, Set<String> requiredForStore ) {
        requiredReaders.put( storeName, Collections.unmodifiableSet( new HashSet<String>( requiredForStore ) ) );
    }

    /**
     * Add a store config to the provider, overriding any existing values.
     * 
     * @param storeName
     * @param cfg
     * @throws InvalidConfigException if the config is not valid
     */
    public void putStoreConfig(String storeName, SeqStoreConfig cfg) throws InvalidConfigException {
        storeMap.put(storeName, cfg.getImmutableConfig() );
    }

    /**
     * Add a reader config to the provider, overriding any existing values.
     * 
     * @param storeName
     * @param readerId
     * @param cfg
     * @throws InvalidConfigException
     */
    public void putReaderConfig(String storeName, String readerId, SeqStoreReaderConfig<InfoType> cfg) throws InvalidConfigException {
        readerMap.put(storeName + SEPERATOR + readerId, cfg.getImmutableConfig() );
    }
    
    public static SeqStoreReaderConfig<BasicInflightInfo> newBasicInflightReaderConfig() {
    	return new SeqStoreReaderConfig<BasicInflightInfo>(BasicInflightInfoFactory.DEFAULT_INSTANCE);
    }
    
    public static final SeqStoreReaderImmutableConfig<BasicInflightInfo> DEFAULT_BASIC_INFLIGHT_READER_CONFIG =
    	newBasicInflightReaderConfig().getImmutableConfig();
    
    public static BasicConfigProvider<BasicInflightInfo> newBasicInflightConfigProvider() {
    	return new BasicConfigProvider<BasicInflightInfo>(
    			SeqStoreConfig.getDefaultConfig(), DEFAULT_BASIC_INFLIGHT_READER_CONFIG);
    }
}
