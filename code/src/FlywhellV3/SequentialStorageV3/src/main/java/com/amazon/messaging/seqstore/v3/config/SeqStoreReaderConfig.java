package com.amazon.messaging.seqstore.v3.config;

import com.amazon.messaging.seqstore.v3.InflightInfoFactory;
import com.amazon.messaging.seqstore.v3.exceptions.InvalidConfigException;

import lombok.Data;
import lombok.ToString;

@Data
@ToString(includeFieldNames=true)
public class SeqStoreReaderConfig<InfoType> implements SeqStoreReaderConfigInterface<InfoType> {
    
    private long startingMaxMessageLifetime = -1;
    
    private int maxInflightTableMessageCountLimit = 10000;

    private InflightInfoFactory<InfoType> inflightInfoFactory;
    
    public SeqStoreReaderConfig(SeqStoreReaderConfigInterface<InfoType> config) {
        this.startingMaxMessageLifetime = config.getStartingMaxMessageLifetime();
        this.maxInflightTableMessageCountLimit = config.getMaxInflightTableMessageCountLimit();
        this.inflightInfoFactory = config.getInflightInfoFactory();
    }
    
    public SeqStoreReaderConfig(InflightInfoFactory<InfoType> inflightInfoFactory) {
    	this.inflightInfoFactory = inflightInfoFactory;
    }

    /**
     * Set how far back in time in milliseconds a reader should start from. Any
     * messages older than the value of this config will be skipped by this
     * reader when it is created/loaded. Messages newer than this will be delivered if
     * they have not already been delivered. If the value is less than 0 or is greater than 
     * the maxMessageLifetime for the store it will be treated as if it is  
     * the maxMessageLifetime. If both this and maxMessageLifeTime are less than 0 then 
     * all messages in the store will be delivered if they have not already been
     * delivered. The default is -1.
     */
    public final void setStartingMaxMessageLifetime(long startingMaxMessageLifetime) {
        this.startingMaxMessageLifetime = startingMaxMessageLifetime;
    }
    
    /**
     * Sets the in-flight info factory used to create/update the in-flight
     * message info and set the re-delivery timeout on every dequeue from
     * this reader.
     * 
     * @param inflightInfoFactory InflightInfoFactory to be used by this reader.
     */
    public final void setInflightInfoFactory(InflightInfoFactory<InfoType> inflightInfoFactory) {
    	if (inflightInfoFactory == null) {
    		throw new IllegalArgumentException("The in-flight info factory for a reader must be non-null.");
    	}
    	this.inflightInfoFactory = inflightInfoFactory;
    }
    
    /**
     * Does a sanity check of the config. Returns a string describing the config error or null
     * if the config is valid. This function currently always returns null.
     * 
     * @return A string describing why the config is invalid or null if the config is valid
     */
    public String validate() {
        return null;
    }
    
    /**
     * Constructs an immutable config object from this object after validating it.
     * 
     * @return An immutable copy of this config
     * @throws InvalidConfigException if the config is not valid according to {@link #validate()}
     */
    public SeqStoreReaderImmutableConfig<InfoType> getImmutableConfig() throws InvalidConfigException {
        String validResult = validate();
        if( validResult != null ) throw new InvalidConfigException( validResult );
        
        return new SeqStoreReaderImmutableConfig<InfoType>(
                inflightInfoFactory, startingMaxMessageLifetime, maxInflightTableMessageCountLimit );
                
    }
    
    /**
     * Set the maximum number of messages the in-flight table of this reader can have at once.
     * @param maxInflightTableMessageCountLimit
     */
    public void setMaxInflightTableMessageCountLimit(int maxInflightTableMessageCountLimit) {
    	this.maxInflightTableMessageCountLimit = maxInflightTableMessageCountLimit;
    }
}
