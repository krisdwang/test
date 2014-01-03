package com.amazon.messaging.seqstore.v3.store;

import com.amazon.messaging.impls.AlwaysIncreasingClock;
import com.amazon.messaging.interfaces.Clock;
import com.amazon.messaging.seqstore.v3.internalInterface.StoreId;
import com.amazon.messaging.utils.MathUtil;


/**
 * A clock that can track the offsets from the primary clock for specific stores.
 * 
 * @author stevenso
 *
 */
public class StoreSpecificClock implements Clock {
    private final Clock localClock;
    
    public StoreSpecificClock(Clock localClock) {
        this.localClock = localClock;
    }

    @Override
    public long getCurrentTime() {
        return localClock.getCurrentTime();
    }
    
    public Clock getLocalClock() {
        return localClock;
    }
    
    /**
     * Return a new storeClock. The returned clock will reflect changes 
     * to the peers offset from current time as much as possible without
     * going backwards. Note that two clocks returned from this function
     * are not guaranteed to be the same even for the same StoreId.
     * 
     * @param storeId
     * @return
     */
    public Clock getStoreClock(final StoreId storeId) {
        return new AlwaysIncreasingClock(
            new Clock() {
                @Override
                public long getCurrentTime() {
                    return getStoreTime( storeId );
                }
            } );
    }
    
    /**
     * Return the how many ms to add to the primary clock to get the store time. This implementation
     * always returns 0, subclasses should override this function to have stores offset from the 
     * main clock.
     * 
     * @return how many ms to add to the primary clock to get the store time.
     */
    protected long getStoreTimeOffset(StoreId storeId) {
        return 0;
    }
    
    /**
     * Return the current time for the given store. Returns the current time plus the result 
     * of {@link #getStoreTimeOffset(StoreId)}.
     * 
     * @param storeId
     * @return
     */
    public long getStoreTime(StoreId storeId) {
        return convertLocalTimeToStoreTime( storeId, getCurrentTime() );
    }
    
    public long convertStoreTimeToLocalTime(StoreId storeId, long time) {
        return MathUtil.addNoOverflow( time, -getStoreTimeOffset(storeId) );
    }
    
    public long convertLocalTimeToStoreTime(StoreId storeId, long time) {
        return MathUtil.addNoOverflow( time, getStoreTimeOffset(storeId) );
    }

}
