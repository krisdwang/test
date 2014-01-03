package com.amazon.messaging.utils.collections;



import java.util.Queue;
import java.util.Random;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import net.jcip.annotations.Immutable;
import net.jcip.annotations.ThreadSafe;



/**
 * This is an optimized queue, which uses a singly-linked list of arrays of fixed size as the underlying data structure.
 * It only supports a subset of the Java Queue functionality. Enqueues happen at the head, dequeues take elements from
 * the tail and random lookups are not supported. The main reason to use it is the {@link Removable} interface, which
 * allows removals to take O(1) time as opposed to the Java {@link Queue} implementations, which can take time O(N).
 * <p>
 * The data structure is thread-safe, but is not bounded so callers need to manage capacity.
 * <p>
 * Memory used by the structure (on a 64-bit JVM) seems to be on the order of:
 * <p>
 * Class : 24 bytes (header) + 4 + 8 + 8 + 4 + 4 + 8 (private members) = 60 bytes
 * <p>
 * Per-block : 24 bytes (header) + 8 + 8 + 4 (private members) + 32 bytes (array overhead) + 16 x 8 bytes (block
 * contents) = 204 bytes
 * <p>
 * So, in total : ~264 bytes
 * 
 * @param <T> Type of the contained entries.
 */
@ThreadSafe
public final class LinkedArrayBlockQueue<T> {

    // The default for this value is derived from the p10 value of the StoreOutstandingRequestsCount metric on the
    // Backend and it has been optimized for this case.
    //
    final static int DEFAULT_BLOCK_SIZE = 8;

    private static final class Block<T> {
        private final T[] data;

        private Block<T> next = null;
        private int size = 0;

        @SuppressWarnings ("unchecked")
        Block (int blockSize) {
            data = (T[]) new Object[blockSize];
        }
    }

    // What should be the size of the individual blocks
    //
    private final int blockSize;

    // Elements are enter at the head and leave from the tail
    //
    private Block<T> head;
    private Block<T> tail;

    // Indexes one past the last valid element in the head block. I.e, there is no valid element at this location. If
    // its value is zero, then the queue is empty.
    //
    private int nextInHead = 0;

    // Index of the first valid element in the tail block. Only goes forward till it gets to the end of the block and
    // then it starts from the beginning of the next block. If -1, then the queue is empty
    //
    private int firstInTail = -1;

    // Number of elements in the collection
    //
    private volatile int size = 0;

    /**
     * Represents an interface permitting a specific, non-tail object to be removed from this collection in O(1) time as
     * opposed to O(N), which the {@link Queue#remove(Object)} call offers.
     */
    public static interface Removable {

        /**
         * An idempotent operation, which removes the specific element from the collection, if it is present there and
         * has no effect on the queue, if it is not.
         * 
         * @return TRUE if the element was found and removed, FALSE if the element was not present in the collection.
         */
        public boolean remove ();
    }

    public LinkedArrayBlockQueue () {
        this (DEFAULT_BLOCK_SIZE);
    }

    public LinkedArrayBlockQueue (int blockSize) {
        this.blockSize = blockSize;

        head = new Block<T> (blockSize);
        tail = head;
    }

    /**
     * Inserts an element at the head of this queue. Same semantics as {@link Queue#offer(Object)}.
     * 
     * @param e Element to insert.
     * @return A {@link Removable} interface, which can be used to remove the added element in O(1) time.
     */
    public synchronized Removable offer (T e) {
        if (nextInHead == blockSize) {
            head = head.next = new Block<T> (blockSize);
            nextInHead = 0;
        }
        if (firstInTail < 0) firstInTail = nextInHead;

        head.data[nextInHead] = e;
        head.size++;

        final Remover remover = new Remover (head, nextInHead);
        nextInHead++;
        size++;

        return remover;
    }

    /**
     * Returns, but does not remove the oldest element in this queue (the element in the tail). Same semantics as
     * {@link Queue#peek()}.
     * 
     * @return Oldest element, or null if the queue is empty.
     */
    public synchronized T peek () {
        if (isEmpty ()) return null;

        return tail.data[firstInTail];
    }

    /**
     * Retrieves and removes the oldest element in the queue (the element in the tail). Subsequently calling
     * {@link Removable#remove()} on the {@link Removable}, which references this element will have no effect and will
     * return false.
     * 
     * @return Oldest element, or null if the queue is empty.
     */
    public synchronized T poll () {
        if (isEmpty ()) return null;

        T ref = tail.data[firstInTail];

        removeInternal (tail, firstInTail);

        return ref;
    }
    
    /**
     * Retrieves and removes a random element in the queue. Subsequently calling {@link Removable#remove()} on 
     * the {@link Removable}, which references this element will have no effect and will return false.
     * 
     * @return random element or null if the queue is empty 
     */
    public synchronized T takeRandom(Random random) {
        return getRandom(random, true);
    }
    
    /**
     * Retrieves a random element in the queue. This runs in O(n/block size) time 
     * 
     * @return random element or null if the queue is empty 
     */
    public T getRandom(Random random) {
        return getRandom(random, false);
    }
    
    private synchronized T getRandom(Random random, boolean remove) {
        if (isEmpty ()) return null;
        int element = random.nextInt(size);
        
        int remaining = element;
        Block<T> currentBlock = tail;
        for(;;) {
            assert remaining >= 0;
            if( currentBlock.size <= remaining ) {
                remaining -= currentBlock.size;
                currentBlock = currentBlock.next;
            } else {
                for( int pos = 0; pos < currentBlock.data.length; ++pos ) {
                    T curVal = currentBlock.data[pos];
                    if( curVal != null ) {
                        if( remaining == 0 ) {
                            if( remove ) { 
                                removeInternal( currentBlock, pos );
                            }
                            return curVal;
                        }
                        remaining--;
                    }
                }
                assert false : "Did not find element in block that claimed to have enough elements.";
            }
        }
    }

    /**
     * Retrieves, but does not remove the newest element in the queue (the element in the head).
     * 
     * @return Newest element, or null if the queue is empty;
     */
    public synchronized T head () {
        if (isEmpty ()) return null;

        return head.data[nextInHead - 1];
    }

    /**
     * Checks whether there are any elements in the collection.
     */
    public synchronized boolean isEmpty () {
        return (firstInTail < 0);
    }

    /**
     * Returns the number of elements in the collection. This method is approximate, because the collection may be
     * changing while the check is made.
     */
    public int size () {
        return size;
    }

    private synchronized boolean removeInternal (Block<T> entry, int idx) {
        if (entry.data[idx] == null) return false;

        entry.data[idx] = null;
        entry.size--;
        size--;

        boolean tailChanged = false;
        
        while (tail.size == 0 && tail.next != null ) {
            tailChanged = true;
            tail = tail.next;
        }
        
        assert ( tail.next != null ) || ( head == tail );
        
        // Reuse head if possible
        if( head.size == 0 ) {
            nextInHead = 0;
        }

        if( tailChanged || ( entry == tail && idx == firstInTail ) ) {
            if (tail.size > 0) {
                int start = tailChanged ? 0 : firstInTail + 1;
                
                boolean changedFirst = false;
                for (int first = start; first < blockSize; first++) {
                    if (tail.data[first] != null) {
                        firstInTail = first;
                        changedFirst = true;
                        break;
                    }
                }
                
                assert changedFirst;
            } else {
                firstInTail = -1;
            }
        }

        return true;
    }

    @Override
    public synchronized String toString () {
        StringBuilder sb = new StringBuilder ('[');
        Block<T> entry = tail;
        while (entry != null) {
            for (T o : entry.data) {
                if (o != null) {
                    sb.append (o).append (',');
                }
            }
            entry = entry.next;
        }
        sb.append (']');

        return sb.toString ();
    }

    @RequiredArgsConstructor (access = AccessLevel.PRIVATE)
    @Immutable
    private final class Remover implements Removable {

        private final Block<T> entry;
        private final int idx;

        @Override
        public boolean remove () {
            return removeInternal (entry, idx);
        }

        @Override
        public String toString () {
            return entry.data[idx].toString ();
        }
    }
}