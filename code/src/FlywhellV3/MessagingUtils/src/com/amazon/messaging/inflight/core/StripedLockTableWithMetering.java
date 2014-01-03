package com.amazon.messaging.inflight.core;

public class StripedLockTableWithMetering<K, LockType> extends StripeLockTable<K, LockType> {

    public StripedLockTableWithMetering(int concurrencyLevel, ConstructorCallback<LockType> ccb) {
        super(concurrencyLevel, ccb);
        accesses = new int[this.segments.length];
    }

    private final int[] accesses;

    @Override
    public LockType get(K key) {
        int hash = hash(key.hashCode());
        int index = (hash >>> segmentShift) & segmentMask;

        accesses[index] += 1;

        return super.get(key);
    }
}
