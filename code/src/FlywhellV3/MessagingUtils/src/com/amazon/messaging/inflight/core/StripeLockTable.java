package com.amazon.messaging.inflight.core;

/* copied from
 * @(#)ConcurrentHashMap.java	1.21 07/01/02
 */

public class StripeLockTable<K, LockType> {

    private static final long serialVersionUID = 7249069246763182397L; // TODO
                                                                       // change
                                                                       // this?

    /* ---------------- Constants -------------- */

    /**
     * The default concurrency level for this table, used when not otherwise
     * specified in a constructor.
     */
    static final int DEFAULT_CONCURRENCY_LEVEL = 16;

    /**
     * The maximum number of segments to allow; used to bound constructor
     * arguments.
     */
    static final int MAX_SEGMENTS = 1 << 16; // slightly conservative

    /* ---------------- Fields -------------- */

    /**
     * Mask value for indexing into segments. The upper bits of a key's hash
     * code are used to choose the segment.
     */
    final int segmentMask;

    /**
     * Shift value for indexing within segments.
     */
    final int segmentShift;

    /**
     * The segments, each of which is a specialized hash table
     */
    final LockType[] segments;

    /* ---------------- Small Utilities -------------- */

    /**
     * Applies a supplemental hash function to a given hashCode, which defends
     * against poor quality hash functions. This is critical because
     * ConcurrentHashMap uses power-of-two length hash tables, that otherwise
     * encounter collisions for hashCodes that do not differ in lower or upper
     * bits.
     */
    final protected static int hash(int h) {
        // Spread bits to regularize both segment and index locations,
        // using variant of single-word Wang/Jenkins hash.
        h += (h << 15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h << 3);
        h ^= (h >>> 6);
        h += (h << 2) + (h << 14);
        return h ^ (h >>> 16);
    }

    /**
     * Returns the segment that should be used for key with given hash
     * 
     * @param hash
     *            the hash code for the key
     * @return the segment
     */
    final private LockType segmentFor(int hash) {
        return segments[(hash >>> segmentShift) & segmentMask];
    }

    /* ---------------- Inner Classes -------------- */

    /**
     * ConcurrentHashMap list entry. Note that this is never exported out as a
     * user-visible Map.Entry. 
     */
    static final class HashEntry<K, V> {

        final K key;

        final int hash;

        final HashEntry<K, V> next;

        HashEntry(K key, int hash, HashEntry<K, V> next) {
            this.key = key;
            this.hash = hash;
            this.next = next;
        }

        @SuppressWarnings("unchecked")
        static final <K, V> HashEntry<K, V>[] newArray(int i) {
            return new HashEntry[i];
        }
    }

    /* ---------------- Public operations -------------- */

    /**
     * @param concurrencyLevel
     *            the estimated number of concurrently updating threads. The
     *            implementation performs internal sizing to try to accommodate
     *            this many threads.
     * @throws IllegalArgumentException
     *             if the concurrencyLevel is nonpositive.
     */
    @SuppressWarnings("unchecked")
    public StripeLockTable(int concurrencyLevel, ConstructorCallback<LockType> ccb) {
        if (concurrencyLevel <= 0)
            throw new IllegalArgumentException();

        if (concurrencyLevel > MAX_SEGMENTS)
            concurrencyLevel = MAX_SEGMENTS;

        // Find power-of-two sizes best matching arguments
        int sshift = 0;
        int ssize = 1;
        while (ssize < concurrencyLevel) {
            ++sshift;
            ssize <<= 1;
        }
        segmentShift = 32 - sshift;
        segmentMask = ssize - 1;
        this.segments = (LockType[]) new Object[ssize];

        for (int i = 0; i < this.segments.length; ++i)
            this.segments[i] = ccb.construct();
        // new ReentrantLock(true);//TODO
    }

    //
    // public StripeLockTable() {
    // this(DEFAULT_CONCURRENCY_LEVEL);
    // }

    /**
     * @param key
     * @return the lock that is shared by keys which hash to it
     */
    public LockType get(K key) {
        int hash = hash(key.hashCode());
        return segmentFor(hash);
    }
    // /**
    // *
    // * @param index
    // * @param value
    // * @return
    // */
    // public LockType put(int index, LockType value) {
    // if (value == null)
    // throw new NullPointerException();
    // return segments[index] = value;
    // }

}
