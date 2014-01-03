package com.amazon.messaging.util;

import lombok.Getter;
import lombok.Setter;

import com.amazon.messaging.seqstore.v3.config.ConfigUnavailableException;
import com.amazon.messaging.seqstore.v3.config.SeqStoreConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStoreImmutableConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStoreReaderImmutableConfig;

public class ThrowingConfigProvider extends BasicConfigProvider<BasicInflightInfo> {
    @Getter @Setter
    private volatile boolean failStoreGets = false;
    
    @Getter @Setter
    private volatile boolean failReaderGets = false;
    
    @Getter
    private volatile ConfigUnavailableException lastException;
    
    public ThrowingConfigProvider() {
        super(SeqStoreConfig.getDefaultConfig(), 
              BasicConfigProvider.newBasicInflightReaderConfig().getImmutableConfig() );
    }
    
    
    @Override
    public SeqStoreReaderImmutableConfig<BasicInflightInfo> getReaderConfig(
            String storeName, String readerName)
            throws ConfigUnavailableException 
    {
        if( failReaderGets ) {
            ConfigUnavailableException ex = new ConfigUnavailableException( "Store Config is unavailable." );
            lastException = ex;
            throw ex;
        }
        
        return super.getReaderConfig(storeName, readerName);
    }
    
    @Override
    public SeqStoreImmutableConfig getStoreConfig(String storeName) throws ConfigUnavailableException {
        if( failStoreGets ) {
            ConfigUnavailableException ex = new ConfigUnavailableException( "Reader Config is unavailable." );
            lastException = ex;
            throw ex;
        }
        
        return super.getStoreConfig(storeName);
    }
}