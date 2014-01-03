package com.amazon.messaging.utils;



import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;



/**
 * A Factory and Map for Stores and Readers. Abstracts creation and storage of children while ensuring we are doing so
 * atomically and only create each child once. This class does not support null children.
 * 
 * @author stevenso
 */
public abstract class ChildrenManager<Key, ChildType, CreateExceptionType extends Exception, GetExceptionType extends Exception> {

    private static final Log log = LogFactory.getLog(ChildrenManager.class);
    
    // A subclass of SettableFuture just in case ChildrenManager is used to store SettableFutures.
    private static final class ChildFuture<ChildType> extends SettableFuture<ChildType> {}
    
    public static class ChildrenManagerClosedException extends Exception {
        private static final long serialVersionUID = 1L;
    }
    
    protected abstract class ChildFactory<ExceptionType extends Exception> implements Callable<ChildType> {
        @Override
        public abstract ChildType call() throws ExceptionType;
    }
    
    private class ValuesIterator implements Iterator<ChildType> {
        private final Iterator<Object> baseItr;
        
        private ChildType next;
        
        public ValuesIterator() {
            this.baseItr = children.values().iterator();
        }

        @Override
        public boolean hasNext() {
            // Skip empty values
            while( next == null && baseItr.hasNext() ) {
                next = getRealValue(null, baseItr.next());
            }
            
            return next != null;
        }

        @Override
        public ChildType next() {
            if( !hasNext() ) throw new NoSuchElementException();
            
            ChildType retval = next;
            next = null;
            return retval;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
        
    }
    
    private class ValuesCollection extends AbstractCollection<ChildType> {
        @Override
        public int size() {
            return children.size();
        }
        
        @Override
        public boolean isEmpty() {
            return children.isEmpty();
        }
        
        @Override
        public boolean contains(Object o) {
            return children.containsValue(o);
        }
        
        @Override
        public Iterator<ChildType> iterator() {
            return new ValuesIterator();
        }
    }
    
    private final ConcurrentMap<Key, Object> children;
    
    private final AtomicBoolean closed = new AtomicBoolean(false);
    
    private final CountDownLatch closeLatch = new CountDownLatch(1);

    public ChildrenManager() {
        children = new ConcurrentHashMap<Key, Object>();
    }
    
    public ChildrenManager(int initialCapacity, float loadFactor, int concurrencyLevel) {
        children = new ConcurrentHashMap<Key, Object>(initialCapacity, loadFactor, concurrencyLevel);
    }
    
    public ChildrenManager(Map<Key, ChildType> initialValues) {
        children = new ConcurrentHashMap<Key, Object>(initialValues.size());
        children.putAll(initialValues);
    }
    
    public ChildrenManager(Map<Key, ChildType> initialValues, float loadFactor, int concurrencyLevel) {
        children = new ConcurrentHashMap<Key, Object>(initialValues.size(), loadFactor, concurrencyLevel);
        children.putAll(initialValues);
    }
    
    protected void checkOpen() throws ChildrenManagerClosedException {
        if( closed.get() ) throw new ChildrenManagerClosedException();
    }
    
    public boolean isClosed() {
        return closed.get();
    }

    @SuppressWarnings("unchecked")
    private ChildType getRealValue(Key childId, Object child) {
        if( child instanceof ChildFuture ) {
            ChildType retval;
            boolean interrupted = false;
            for(;;) {
                try {
                    retval = ( ( ChildFuture<ChildType> ) child ).get();
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                    String strChildId = childId == null ? "<unknown>" : childId.toString();
                    log.info( "Interrupted waiting for " + strChildId + " to be created." );
                } catch (ExecutionException e) {
                    throw new RuntimeException("ChildFutures should never throw!", e); 
                }
            }
            if( interrupted ) {
                Thread.currentThread().interrupt();
            }
            return retval;
        } else {
            return (ChildType) child;
        }
    }
    
    /**
     * Get or create the child with the given id. If the child is not in memory
     * this calls {@link #getOrCreate(Key, ChildFactory)} with a factory
     * that creates the value using {@link #createChild(Object)}
     */
    public ChildType getOrCreate(final Key childId) 
        throws ChildrenManagerClosedException, CreateExceptionType 
    {
        return getOrCreate( childId, new ChildFactory<CreateExceptionType>() {
            @Override
            public ChildType call() throws CreateExceptionType {
                return createChild(childId);
            }
        });
    }
        
    /**
     * Get or create the child with the given key. If the child does not
     * exist it is created with the specified factory instead of 
     * the default factory.
     */
    protected <ExceptionType extends Exception > ChildType getOrCreate(
        final Key childId, ChildFactory<ExceptionType> factory) 
        throws ChildrenManagerClosedException, ExceptionType 
    {
        checkOpen();
        
        ChildType child = getRealValue( childId, children.get( childId ) );
        
        if( child == null ) {
            ChildFuture<ChildType> future = new ChildFuture<ChildType>();
            future.setRunningThread(Thread.currentThread()); // For debugging
            
            Object otherChild;
            
            // Loop till another thread succesfully creates the child or 
            // this thread is responsible for the creation
            do {
                otherChild = children.putIfAbsent( childId, future );
                if( otherChild != null ) {
                    // If another thread beat this one to adding the entry we need to
                    //  see if that thread succeeds and if so take its value
                    child = getRealValue( childId, otherChild );
                }
            } while( otherChild != null && child == null );
            
            if( child == null ) {
                try {
                    // Check again after the future is in the map. Otherwise
                    //  a value could be created after close has returned
                    checkOpen(); 

                    child = factory.call();
                    if (child == null) {
                        throw new IllegalArgumentException ("Null children are not allowed in "
                                + ChildrenManager.class.getName () + ". Null child was returned for " + childId);
                    }

                    boolean replaceResult = children.replace(childId, future, child);
                    assert (replaceResult == true);
                    future.set( child );

                    try {
                        newChildCreated (childId, child);
                    } catch (RuntimeException e) {
                        log.warn ("New child notification threw unexpected exception.", e);
                    }
                } finally {
                    // If creation failed set the future to null to wake any threads
                    //  waiting for the creation to complete
                    if( !future.isDone() ) {
                        boolean removeResult = children.remove( childId, future );
                        assert removeResult;
                        future.set( null );
                    }
                }
            }
        }
        
        return child;
    }

    protected abstract ChildType createChild(Key childId) throws CreateExceptionType;
    
    /**
     * Provides means to register a callback with this children manager in order to notify the caller that a new entry
     * has been inserted. The callback will be invoked each time an entry is missing in the cache and needs to be
     * created through a call to {@link #createChild(Object)}.
     * <p>
     * It is not okay to invoke any methods on the collection from within this notification in order to avoid possible
     * infinite recursion or self deadlock situations. For example, in the presence of a concurrent delete and reinsert
     * of the same key, calling {@link #get(Object)} from within this notification may potentially cause liveness
     * problem and infinite recursion, so it's best to be avoided.
     * <p>
     * Note that there is no guarantee that calling {@link #get(Object)} after this notification is invoked will return
     * the same child object (or any child object at all), because of concurrency.
     * 
     * @param key The key of the created child.
     * @param child The child object which was created.
     */
    protected void newChildCreated (Key key, ChildType child) { }

    /**
     * Get an existing child. Sub classes of the ChildrenManager may override this to 
     * load objects that exist but that have not been loaded into memory.
     *  
     * @param childId the id of the child to get
     * @return the already existing child with the matching id
     * @throws GetExceptionType
     * @throws ChildrenManagerClosedException 
     */
    public ChildType get(Key childId) throws GetExceptionType, ChildrenManagerClosedException {
        checkOpen();
        return getFromMap( childId );
    }
    
    private ChildType getFromMap(Key childId)  {
        return getRealValue(childId, children.get(childId) );
    }
    
    /**
     * Remove childId from the manager only if it is currently mapped to child. Entries
     * that are in the process of being created do not have a value that can be matched
     * and so cannot be removed by this function. 
     * 
     * @param childId the id of the child to remove
     * @param child the expected current value of childId
     * @return true if the remove happened, else false
     * @throws ChildrenManagerClosedException 
     */
    protected boolean remove(Key childId, ChildType child) throws ChildrenManagerClosedException {
        checkOpen();
        
        return children.remove(childId, child);
    }

    /**
     * Return an unmodifiable view of the keys for the children managed by this ChildrenManager.
     * The keys may include objects in the process of being created. After the manager
     * is closed this view will be empty.
     *  
     * @throws ChildrenManagerClosedException 
     */
    public Set<Key> keySet() {
        return Collections.unmodifiableSet( children.keySet() );
    }

    /**
     * Return an unmodifiable collection of values. This collection is backed by the manager
     *  and will reflect any changes to the values in the manager. Iterating over the collection
     *  may block on the creation of partially created values.
     * <p>
     * After the manager is closed this collection becomes empty.
     */
    public Collection<ChildType> values() {
        if( isClosed() ) return Collections.emptySet();
        
        return new ValuesCollection();
    }
    
    
    /**
     * Close the ChildrenManager. After this is called all operations
     * on the ChildrenManager will throw ChildrenManagerClosedException. 
     * <p>
     * This function will block until all objects that are in the middle
     * of creation have been created. Other operations in progress
     * while this runs may throw ChildrenManagerClosedException or
     * may complete normally.
     * <p>
     * This function returns a Map of all children that were in 
     * the manager when close was called. If close is called in parallel
     * the second call to close blocks until the first is complete and 
     * then returns an empty map. It is guaranteed that
     * that no child was left out of the map even if its
     * create was in progress while close() was running. It 
     * is not guaranteed that there are not extra entries in the map
     * that were removed while close was running. 
     * 
     * @return a {@link Map} of all children that were in the manager 
     *  when close was called
     */
    protected Map<Key,ChildType> closeManager() {
        if( closed.compareAndSet(false, true)) {
            
            Map<Key, ChildType> map = new HashMap<Key, ChildType>();
            for( Map.Entry<Key, Object> entry : children.entrySet() ) {
                ChildType realVal = getRealValue(entry.getKey(), entry.getValue());
                if( realVal != null ) {
                    map.put( entry.getKey(), realVal );
                }
            }
            
            assert children.size() == map.size();
            children.clear();
            
            closeLatch.countDown();
            return map;
        } else {
            boolean interrupted = false;
            for(;;) {
                try {
                    closeLatch.await();
                    if( interrupted ) Thread.currentThread().interrupt();
                    return Collections.emptyMap();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
    }
}
