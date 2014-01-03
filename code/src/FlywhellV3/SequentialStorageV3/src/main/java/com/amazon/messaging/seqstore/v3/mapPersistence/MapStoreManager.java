package com.amazon.messaging.seqstore.v3.mapPersistence;

import com.amazon.coral.metrics.NullMetricsFactory;
import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.v3.CheckpointProvider;
import com.amazon.messaging.seqstore.v3.config.ConfigProvider;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdGenerator;
import com.amazon.messaging.seqstore.v3.internal.AckIdSourceFactory;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.DefaultAckIdSourceFactory;
import com.amazon.messaging.seqstore.v3.internal.SeqStoreManagerV3;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.seqstore.v3.store.StorePersistenceManager;
import com.amazon.messaging.seqstore.v3.store.StoreSpecificClock;
import com.amazon.messaging.utils.Scheduler;

/**
 * A manager to create stores that will be in memory and not persist anything
 * across restarts.
 * 
 * @author kaitchuc
 */
public class MapStoreManager<InfoType> extends SeqStoreManagerV3<InfoType> {
    public MapStoreManager(ConfigProvider<InfoType> configProvider, Clock clock) 
            throws SeqStoreDatabaseException
    {
        this( configProvider, 
                new DefaultAckIdSourceFactory( new AckIdGenerator( clock.getCurrentTime() ), clock), 
                new StoreSpecificClock(clock));
    }
    
    public MapStoreManager(ConfigProvider<InfoType> configProvider, Scheduler scheduler, Clock clock) 
            throws SeqStoreDatabaseException
    {
        this( configProvider, new NonPersistentBucketCreator<InfoType>(), scheduler, clock);
    }
    
    public MapStoreManager(
        ConfigProvider<InfoType> configProvider, StorePersistenceManager persistenceManager, Clock clock) 
            throws SeqStoreDatabaseException
    {
        this( configProvider, persistenceManager,  new Scheduler("Inactive map store thread", 5), clock );
    }
    
    public MapStoreManager(
        ConfigProvider<InfoType> configProvider, StorePersistenceManager persistenceManager, 
        Scheduler scheduler, Clock clock) 
            throws SeqStoreDatabaseException
    {
        super( persistenceManager, new DefaultAckIdSourceFactory( new AckIdGenerator( clock.getCurrentTime() ), clock),
               new StoreSpecificClock(clock), configProvider, null, scheduler,
               new NullMetricsFactory() );
    }

    public MapStoreManager(ConfigProvider<InfoType> configProvider, 
                           AckIdSourceFactory ackIdSourceFactory, StoreSpecificClock clock)
            throws SeqStoreDatabaseException
    {
        this( configProvider, null, ackIdSourceFactory, clock );
    }
    
    public MapStoreManager(ConfigProvider<InfoType> configProvider, 
                           CheckpointProvider<StoreId, AckIdV3, AckIdV3, InfoType> checkpointProvider,
                           AckIdSourceFactory ackIdSourceFactory, StoreSpecificClock clock) 
            throws SeqStoreDatabaseException
    {
        super( new NonPersistentBucketCreator<InfoType>(), ackIdSourceFactory, clock,
               configProvider, checkpointProvider, new Scheduler("Inactive map store thread", 5),
               new NullMetricsFactory() );
    }
    
    @Override
    public void close() throws SeqStoreException {
        super.close();
        getScheduler().shutdown();
    }
}
