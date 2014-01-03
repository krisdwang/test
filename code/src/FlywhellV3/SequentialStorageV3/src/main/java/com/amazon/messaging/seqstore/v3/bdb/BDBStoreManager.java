package com.amazon.messaging.seqstore.v3.bdb;

import com.amazon.coral.metrics.MetricsFactory;
import com.amazon.coral.metrics.NullMetricsFactory;
import com.amazon.messaging.annotations.TestOnly;
import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.v3.CheckpointProvider;
import com.amazon.messaging.seqstore.v3.config.ConfigProvider;
import com.amazon.messaging.seqstore.v3.config.SeqStoreImmutablePersistenceConfig;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreException;
import com.amazon.messaging.seqstore.v3.internal.AckIdGenerator;
import com.amazon.messaging.seqstore.v3.internal.AckIdSourceFactory;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.DefaultAckIdSourceFactory;
import com.amazon.messaging.seqstore.v3.internal.SeqStoreManagerV3;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.seqstore.v3.store.StoreSpecificClock;
import com.amazon.messaging.utils.Scheduler;

/**
 * A SeqStoreManager who's stores, messages, and readers are persisted to disk
 * so that all state is preserved between restarts.
 * 
 * @author kaitchuc
 */
public class BDBStoreManager<InfoType> extends SeqStoreManagerV3<InfoType> {
	/**
     * Returns true if the BDB has already been created on disk
     * at the location specified by config
     */
    public static boolean bdbExists(SeqStoreImmutablePersistenceConfig persistConfig) {
        return BDBPersistentManager.bdbExists( persistConfig );
    }
    
    @TestOnly
    public static <InfoType> BDBStoreManager<InfoType> createDefaultTestingStoreManager(
            ConfigProvider<InfoType> configProvider, SeqStoreImmutablePersistenceConfig config,
            Clock clock) throws SeqStoreException
    {
        return createStoreManager(
                configProvider, null, config, 
                new DefaultAckIdSourceFactory( new AckIdGenerator(clock.getCurrentTime()), clock), 
                new StoreSpecificClock(clock), new NullMetricsFactory() );
    }
    
    @TestOnly
    public static <InfoType> BDBStoreManager<InfoType> createDefaultTestingStoreManager(
            ConfigProvider<InfoType> configProvider, SeqStoreImmutablePersistenceConfig config,
            AckIdGenerator ackIdGen, Clock clock) throws SeqStoreException
    {
        return createStoreManager(
                configProvider, null, config, 
                new DefaultAckIdSourceFactory( ackIdGen, clock), new StoreSpecificClock(clock),
                new NullMetricsFactory() );
    }
    
    @TestOnly
    public static <InfoType> BDBStoreManager<InfoType> createDefaultTestingStoreManager(
            ConfigProvider<InfoType> configProvider,
            CheckpointProvider<StoreId, AckIdV3, AckIdV3, InfoType> checkpointProvider,
            SeqStoreImmutablePersistenceConfig config,
            AckIdGenerator ackIdGen, Clock clock) throws SeqStoreException
    {
        return createStoreManager(
                configProvider, checkpointProvider, config, 
                new DefaultAckIdSourceFactory( ackIdGen, clock), new StoreSpecificClock(clock),
                new NullMetricsFactory() );
    }
    
    /**
     * Create a BDBStoreManager store manager for testing with the given config, clock and scheduler.
     * Note that the store manager takes ownership of the scheduler and shuts down the scheduler
     * when the store manager is closed
     */
    @TestOnly
    public static <InfoType> BDBStoreManager<InfoType> createDefaultTestingStoreManager(
            ConfigProvider<InfoType> configProvider, SeqStoreImmutablePersistenceConfig config,
            Clock clock, Scheduler scheduler ) throws SeqStoreException
    {
        BDBPersistentManager persistMan = new BDBPersistentManager(config, scheduler);
        
        return new BDBStoreManager<InfoType>(
                configProvider, null, config, 
                new DefaultAckIdSourceFactory( new AckIdGenerator(clock.getCurrentTime()), clock),  
                new StoreSpecificClock(clock), scheduler, (MetricsFactory) new NullMetricsFactory(), persistMan);
    }
    
    public static <InfoType> BDBStoreManager<InfoType> createStoreManager(
            ConfigProvider<InfoType> configProvider, 
            CheckpointProvider<StoreId, AckIdV3, AckIdV3, InfoType> checkpointProvider,
            SeqStoreImmutablePersistenceConfig config,
            AckIdSourceFactory ackIdSourceFactory, StoreSpecificClock clock,
            MetricsFactory metricsFactory ) throws SeqStoreException
    {
        Scheduler commonThreadPool = new Scheduler("Inactive store thread.",5); 
        BDBPersistentManager persistMan = new BDBPersistentManager(config, commonThreadPool);
        
        return new BDBStoreManager<InfoType>(
                configProvider, checkpointProvider, config, ackIdSourceFactory, clock, commonThreadPool, 
                metricsFactory, persistMan);
    }
    
    public BDBStoreManager(ConfigProvider<InfoType> configProvider,
                           CheckpointProvider<StoreId, AckIdV3, AckIdV3, InfoType> checkpointProvider,
                           SeqStoreImmutablePersistenceConfig config, AckIdSourceFactory ackIdSourceFactory,
                           StoreSpecificClock clock, Scheduler commonThreadPool, MetricsFactory metricsFactory,
                           BDBPersistentManager persistMan) 
        throws SeqStoreException 
    {
        super(
                persistMan, ackIdSourceFactory, clock, configProvider, 
                checkpointProvider, commonThreadPool, metricsFactory);
    }
    
    @Override
    public void close() throws SeqStoreException {
        super.close();
        getScheduler().shutdown();
    }
}
