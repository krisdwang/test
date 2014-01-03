package com.amazon.messaging.seqstore.v3.internal;

import java.util.HashMap;
import java.util.Map;

import com.amazon.messaging.seqstore.v3.CheckpointProvider;
import com.amazon.messaging.seqstore.v3.config.ConfigProvider;
import com.amazon.messaging.seqstore.v3.config.ConfigUnavailableException;
import com.amazon.messaging.seqstore.v3.config.SeqStoreReaderImmutableConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreMissingConfigException;
import com.amazon.messaging.seqstore.v3.internalInterface.SeqStoreReaderV3InternalInterface;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.seqstore.v3.store.Store;
import com.amazon.messaging.seqstore.v3.store.StoreSpecificClock;
import com.amazon.messaging.utils.ChildrenManager;

public class SeqStoreReaderManager<InfoType> extends ChildrenManager<String, SeqStoreReaderV3InternalInterface<InfoType>, SeqStoreException, SeqStoreClosedException> {
    
    private final SeqStoreV3<InfoType> store;

    private final Store messages;

    SeqStoreReaderManager(SeqStoreV3<InfoType> store, 
                          Store messages, Map<String, AckIdV3> ackLevels,
                          Map<String, SeqStoreReaderImmutableConfig<InfoType>> readerConfigs) 
          throws SeqStoreDatabaseException, SeqStoreMissingConfigException, SeqStoreClosedException
    {
        super( createStartingReaders(store, messages, ackLevels, readerConfigs), 
               0.75f, 1 );
        
        this.store = store;
        this.messages = messages;
    }

    private static <InfoType> Map<String, SeqStoreReaderV3InternalInterface<InfoType>> 
        createStartingReaders(
                SeqStoreV3<InfoType> store,
                Store messages,
                Map<String, AckIdV3> ackLevels,
                Map<String, SeqStoreReaderImmutableConfig<InfoType>> readerConfigs)
            throws SeqStoreDatabaseException, SeqStoreMissingConfigException, SeqStoreClosedException 
    {
        ConfigProvider<InfoType> configProvider = store.getConfigProvider();
        CheckpointProvider<StoreId, AckIdV3, AckIdV3, InfoType> checkpointProvider
            = store.getCheckpointProvider();
        StoreId storeId = store.getStoreId();
        StoreSpecificClock clock = store.getClock();
        
        Map<String, SeqStoreReaderV3InternalInterface<InfoType>> initialMap = 
            new HashMap<String, SeqStoreReaderV3InternalInterface<InfoType>>();
        for (Map.Entry<String, AckIdV3> e : ackLevels.entrySet()) {
            SeqStoreReaderImmutableConfig<InfoType> readerConfig = 
                readerConfigs.get( e.getKey() );
            if( readerConfig == null ) 
                throw new SeqStoreMissingConfigException("No configuration provided for " + e.getKey() );
            
            SeqStoreReaderV3<InfoType> reader = new SeqStoreReaderV3<InfoType>(
            		storeId, e.getKey(), readerConfig, configProvider, checkpointProvider, 
            		messages, e.getValue(), clock);
            TopDisposableReader<InfoType> disposable = new TopDisposableReader<InfoType>(reader);
            initialMap.put( e.getKey(), disposable );
        }
        
        return initialMap;
    }
    
    @Override
    public SeqStoreReaderV3InternalInterface<InfoType> get(String childId) throws SeqStoreClosedException {
        try {
            return super.get(childId);
        } catch (ChildrenManagerClosedException e) {
            throw new SeqStoreClosedException("SeqStoreReaderManager is closed", e );
        }
    }
    
    @Override
    public SeqStoreReaderV3InternalInterface<InfoType> getOrCreate(String childId) throws SeqStoreException 
    {
        try {
            return super.getOrCreate(childId);
        } catch (ChildrenManagerClosedException e) {
            throw new SeqStoreClosedException("SeqStoreReaderManager is closed", e );
        }
    }
    
    @Override
    protected TopDisposableReader<InfoType> createChild(String childId) throws SeqStoreException {
        ConfigProvider<InfoType> configProvider = store.getConfigProvider();
        CheckpointProvider<StoreId, AckIdV3, AckIdV3, InfoType> checkpointProvider
            = store.getCheckpointProvider();
        StoreId storeId = store.getStoreId();
        StoreSpecificClock clock = store.getClock();
        
        SeqStoreReaderV3<InfoType> reader;
        try {
            reader = new SeqStoreReaderV3<InfoType>(
                    storeId, childId, 
                    configProvider.getReaderConfig( storeId.getGroupName(), childId),
                    configProvider, checkpointProvider, 
            		messages, null, clock);
        } catch (ConfigUnavailableException e) {
            throw new SeqStoreMissingConfigException(
                    "Unable to find configuration for " + storeId + "." + childId, e );
        }
        return new TopDisposableReader<InfoType>(reader);
    }

    public boolean removeReader(String readerId) throws SeqStoreClosedException {
        try {
            SeqStoreReaderV3InternalInterface<InfoType> reader = get(readerId);
            if( reader == null ) return false;
            
            ( ( TopDisposableReader<?>) reader ).close();
            
            CheckpointProvider<StoreId, AckIdV3, AckIdV3, InfoType> checkpointProvider
                = store.getCheckpointProvider();
            if( checkpointProvider != null ) checkpointProvider.readerDeleted(store.getStoreId(), readerId);
            super.remove( readerId, reader );
            return true;
        } catch( ChildrenManagerClosedException e ) {
            throw new SeqStoreClosedException("SeqStoreReaderManager is closed", e );
        }
    }
    
    public void close() {
        Map<String, SeqStoreReaderV3InternalInterface<InfoType>> children = super.closeManager();
        for( SeqStoreReaderV3InternalInterface<InfoType> child : children.values() ) {
            ( ( TopDisposableReader<?>) child ).close();
        }
    }
}
