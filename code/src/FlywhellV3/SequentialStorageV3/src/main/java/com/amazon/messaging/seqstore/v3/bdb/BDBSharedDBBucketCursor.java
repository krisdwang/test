package com.amazon.messaging.seqstore.v3.bdb;

import javax.annotation.concurrent.NotThreadSafe;

import lombok.Getter;
import lombok.Setter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.store.BucketCursor;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.OperationStatus;

import static com.amazon.messaging.seqstore.v3.bdb.BDBSharedDBBucketStore.KEY_BINDING;
import static com.amazon.messaging.seqstore.v3.bdb.BDBSharedDBBucketStore.ENTRY_BINDING;
import static com.amazon.messaging.seqstore.v3.bdb.BDBSharedDBBucketStore.TERMINATOR;
import static com.amazon.messaging.seqstore.v3.bdb.BDBSharedDBBucketStore.PREFIX;
import static com.amazon.messaging.seqstore.v3.bdb.BDBSharedDBBucketStore.AFTER_PREFIX;

@NotThreadSafe
public class BDBSharedDBBucketCursor implements BucketCursor {
    private static final Log log = LogFactory.getLog(BDBSharedDBBucketCursor.class);
    
    private final Database commonDatabase;
    
    private final long bucketSequenceId;
    
    private BDBSharedDBBucketKey currentKey;
    
    private boolean closed;
    
    @Getter @Setter
    private boolean evictFromCacheAfterReading;
    
    public BDBSharedDBBucketCursor(Database commonDatabase, long bucketSequenceId) {
        this.commonDatabase = commonDatabase;
        this.bucketSequenceId = bucketSequenceId;
        this.currentKey = null;
    }
    
    public BDBSharedDBBucketCursor(BDBSharedDBBucketCursor other) {
        this.commonDatabase = other.commonDatabase;
        this.bucketSequenceId = other.bucketSequenceId;
        this.currentKey = other.currentKey;
    }

    @Override
    public void close() throws SeqStoreDatabaseException {
        closed = true;
    }
    
    private void checkNotClosed() {
        if( closed ) throw new IllegalStateException( "Attempt to use closed cursor.");
    }

    @Override
    public BucketCursor copy() throws SeqStoreDatabaseException {
        checkNotClosed();
        
        return new BDBSharedDBBucketCursor(this);
    }
    
    @Override
    public AckIdV3 current() {
        if( currentKey == null ) return null;
        return currentKey.getAckId();
    }
    
    private void throwMissingBoundaryError( boolean advancing ) throws SeqStoreDatabaseException {
        if( advancing ) {
            throw new SeqStoreDatabaseException( 
                    "Missing terminator for bucket " + bucketSequenceId );
        } else {
            throw new SeqStoreDatabaseException( 
                    "Missing prefix for bucket " + bucketSequenceId );
        }
    }
    
    private BDBSharedDBBucketKey getKeyFromResult( DatabaseEntry keyDBEntry, boolean advancing )
            throws SeqStoreDatabaseException
    {
        BDBSharedDBBucketKey key = KEY_BINDING.entryToObject(keyDBEntry);
        if( key.getBucketSequenceId() != bucketSequenceId ) {
           throwMissingBoundaryError(advancing);
        }
        
        return key;
    }
    
    @Override
    public StoredEntry<AckIdV3> nextEntry(AckIdV3 max)
        throws SeqStoreDatabaseException
    {
        checkNotClosed();
        
        CacheMode cacheMode = CacheMode.DEFAULT;
        
        /*
         * TODO: Enable this after upgrading to JE 5. JE 4 requires reading the LN
         * to delete so evicting it hurts performance
         * 
        if( evictFromCacheAfterReading ) {
            cacheMode = CacheMode.EVICT_LN;
        } else {
            cacheMode = CacheMode.DEFAULT;
        }*/
        
        DatabaseEntry keyDBEntry = new DatabaseEntry();
        DatabaseEntry valueDBEntry = new DatabaseEntry();
        
        Cursor cursor = null;
        try {
            CursorConfig cursorConfig = new CursorConfig();
            cursorConfig.setReadUncommitted(true);
            cursor = commonDatabase.openCursor(null, cursorConfig);
            
            cursor.setCacheMode( cacheMode );
            AckIdV3 key = next( max, cursor, false, keyDBEntry, valueDBEntry );
            
            if( key == null ) return null;
            return ENTRY_BINDING.entryToObject(valueDBEntry);
        } finally {
            closeCursor(cursor);
        }
    }
    
    private AckIdV3 next(
        AckIdV3 max, Cursor cursor, boolean cursorIsPositioned, DatabaseEntry keyDBEntry, DatabaseEntry valueDBEntry) 
            throws SeqStoreDatabaseException 
    {
        OperationStatus status;
        
        try {
            if( cursorIsPositioned ) {
                status = cursor.getNext(keyDBEntry, valueDBEntry, null);
            } else if( currentKey != null ) {
                // Use the fact that a serialized AckIdV3 with isInclusive non null comes after
                //  the serialized version with isInclusive null but before any other keys
                BDBSharedDBBucketKey searchKey = 
                        new BDBSharedDBBucketKey(bucketSequenceId, new AckIdV3( currentKey.getAckId(), true ) );
                KEY_BINDING.objectToEntry(searchKey, keyDBEntry);
                status = cursor.getSearchKeyRange(keyDBEntry, valueDBEntry, null);
            } else {
                BDBSharedDBBucketKey searchKey = 
                        new BDBSharedDBBucketKey(bucketSequenceId, AFTER_PREFIX );
                KEY_BINDING.objectToEntry(searchKey, keyDBEntry);
                
                status = cursor.getSearchKeyRange(keyDBEntry, valueDBEntry, null);
            }
            
            if( status == OperationStatus.NOTFOUND ) {
                throwMissingBoundaryError(true);
            }
            
            DatabaseUtil.checkSuccess(status, "getSearchKeyRange");
            
            BDBSharedDBBucketKey newKey = getKeyFromResult(keyDBEntry, true);
            if( newKey.getAckId().equals( TERMINATOR ) ) {
                return null;
            }
            
            if (max != null && newKey.getAckId().compareTo(max) > 0) {
                // At the time of getNext there were no records between the
                // current position and max so return null
                return null;
            }

            currentKey = newKey;
            return newKey.getAckId();
        } catch (DatabaseException ex) {
            throw DatabaseUtil.translateBDBException("Failed advancing cursor:" + ex.getMessage(), ex);
        }
    }

    @Override
    public long advanceToKey(AckIdV3 key) throws SeqStoreDatabaseException {
        checkNotClosed();
        
        DatabaseEntry keyDBEntry = new DatabaseEntry();
        DatabaseEntry valueDBEntry = new DatabaseEntry();
        valueDBEntry.setPartial(0, 0, true );
        
        CacheMode cacheMode = CacheMode.UNCHANGED;
        /*
         * TODO: Enable this after upgrading to JE 5. JE 4 requires reading the LN
         * to delete so evicting it hurts performance
         * 
        if( evictFromCacheAfterReading ) {
            cacheMode = CacheMode.EVICT_LN;
        } else {
            cacheMode = CacheMode.UNCHANGED;
        }*/
        
        Cursor cursor = null;
        try {
            CursorConfig cursorConfig = new CursorConfig();
            cursorConfig.setReadUncommitted(true);
            cursor = commonDatabase.openCursor(null, cursorConfig);
            
            cursor.setCacheMode( cacheMode );
            long count = 0;
            if( next( key, cursor, false, keyDBEntry, valueDBEntry ) != null ) {
                count++;
                while( next( key, cursor, true, keyDBEntry, valueDBEntry ) != null ) count++;
            }
            return count;
        } finally {
            closeCursor(cursor);
        }
    }

    @Override
    public boolean jumpToKey(AckIdV3 key) throws SeqStoreDatabaseException {
        checkNotClosed();
        
        DatabaseEntry keyDBEntry = new DatabaseEntry();
        DatabaseEntry valueDBEntry = new DatabaseEntry();
        valueDBEntry.setPartial(0, 0, true );
        
        CacheMode cacheMode = CacheMode.UNCHANGED;
        /*
         * TODO: Enable this after upgrading to JE 5. JE 4 requires reading the LN
         * to delete so evicting it hurts performance
         * 
        if( evictFromCacheAfterReading ) {
            cacheMode = CacheMode.EVICT_LN;
        } else {
            cacheMode = CacheMode.UNCHANGED;
        }*/
        
        Cursor cursor = null;
        try {
            CursorConfig cursorConfig = new CursorConfig();
            cursorConfig.setReadUncommitted(true);
            cursor = commonDatabase.openCursor(null, cursorConfig);
            cursor.setCacheMode(cacheMode);
            
            AckIdV3 coreAckId = key.getCoreAckId();
            BDBSharedDBBucketKey searchKey = 
                    new BDBSharedDBBucketKey(bucketSequenceId, coreAckId );

            KEY_BINDING.objectToEntry(searchKey, keyDBEntry);
            OperationStatus status = cursor.getSearchKeyRange(keyDBEntry, valueDBEntry, null);

            if( status == OperationStatus.NOTFOUND ) {
                throwMissingBoundaryError(true);
            }
            
            DatabaseUtil.checkSuccess(status, "getSearchKeyRange");
            
            BDBSharedDBBucketKey cursorKey = KEY_BINDING.entryToObject(keyDBEntry);
            if( cursorKey.getAckId().equals( TERMINATOR ) ) {
                // Nothing equal to or after the specified key, so start at the end of the database
                status = cursor.getPrevNoDup(keyDBEntry, valueDBEntry, null);
                
                if( status == OperationStatus.NOTFOUND ) {
                    throwMissingBoundaryError(false);
                }
                
                DatabaseUtil.checkSuccess(status, "getPrevNoDup");
                
                cursorKey = getKeyFromResult(keyDBEntry, false);
                
                if( cursorKey.getAckId().equals( PREFIX ) ) {
                    // Empty bucket
                    assert currentKey == null;
                    return false;
                }
            }

            boolean foundExactKey = cursorKey.getAckId().equals( coreAckId );
            
            if( !foundExactKey || ( key.isInclusive() != null && !key.isInclusive() ) ) {
                while( key.compareTo(cursorKey.getAckId()) < 0 ) {
                    status = cursor.getPrevNoDup(keyDBEntry, valueDBEntry, null);
                    if( status == OperationStatus.NOTFOUND ) {
                        throwMissingBoundaryError(false);
                    }
                    
                    DatabaseUtil.checkSuccess(status, "getPrevNoDup");
                    
                    cursorKey = getKeyFromResult(keyDBEntry, false);
                    
                    if( cursorKey.getAckId().equals( PREFIX ) ) {
                        // At the start of the store
                        cursorKey = null;
                        break;
                    }
                }
            }
            
            currentKey = cursorKey;
            
            return foundExactKey;
        } catch (DatabaseException ex) {
            throw DatabaseUtil.translateBDBException("Failed advancing cursor:" + ex.getMessage(), ex);
        } finally {
            closeCursor(cursor);
        }
    }
    
    private static void closeCursor(Cursor cursor) throws SeqStoreDatabaseException {
        if( cursor == null ) return;
        
        try {
            cursor.close();
        } catch( DatabaseException e ){
            log.error( "Error closing cursor: ", e );
            if( cursor.getDatabase().getEnvironment().isValid() ) {
                // If the environment isn't valid then the failure probably isn't caused by the close
                // and we don't want to hide the point where the actual failure occured.
                throw DatabaseUtil.translateBDBException(e);
            }
        }
    }

    @Override
    public long getDistance(AckIdV3 key) throws SeqStoreDatabaseException {
        checkNotClosed();
        
        if( key == null ) {
            if( currentKey == null ) return 0;
            key = AckIdV3.MINIMUM;
        } else if( currentKey != null && key.equals( currentKey.getAckId() ) ) {
            return 0;
        }
        
        DatabaseEntry keyDBEntry = new DatabaseEntry();
        DatabaseEntry valueDBEntry = new DatabaseEntry();
        valueDBEntry.setPartial(0, 0, true );
        
        Cursor cursor = null;
        try {
            CursorConfig cursorConfig = new CursorConfig();
            cursorConfig.setReadUncommitted(true);
            cursor = commonDatabase.openCursor(null, cursorConfig);
            cursor.setCacheMode( CacheMode.UNCHANGED );
                
            if( currentKey != null ) {
                KEY_BINDING.objectToEntry(currentKey, keyDBEntry);
                OperationStatus status = cursor.getSearchKey(keyDBEntry, valueDBEntry, null);
                
                if( status == OperationStatus.NOTFOUND ) {
                    throw new SeqStoreDatabaseException("Unable to seek to current location" );
                }
                
                DatabaseUtil.checkSuccess(status, "getSearchKey");
            } else {
                BDBSharedDBBucketKey startKey = 
                        new BDBSharedDBBucketKey(bucketSequenceId, PREFIX);
                KEY_BINDING.objectToEntry(startKey, keyDBEntry);
                OperationStatus status = cursor.getSearchKey(keyDBEntry, valueDBEntry, null);
                
                if( status == OperationStatus.NOTFOUND ) {
                    throwMissingBoundaryError(false);
                }
                
                DatabaseUtil.checkSuccess(status, "getSearchKey");
            }
            
            boolean advance;
            if( currentKey == null ) advance = true;
            else advance = key.compareTo( currentKey.getAckId() ) > 0;
            
            long distance = 0;
            for(;;) {
                OperationStatus status;
                if( advance ) {
                    status = cursor.getNextNoDup(keyDBEntry, valueDBEntry, null);
                } else {
                    status = cursor.getPrevNoDup(keyDBEntry, valueDBEntry, null);
                }
                
                if( status == OperationStatus.NOTFOUND ) {
                    throwMissingBoundaryError(advance);
                }
                
                DatabaseUtil.checkSuccess(status, "getNextNoDup|getPrevNoDup");
                
                BDBSharedDBBucketKey cursorKey = getKeyFromResult(keyDBEntry, advance);
                
                AckIdV3 pos = cursorKey.getAckId();
                
                if( ( advance && pos.equals( TERMINATOR ) ) || ( !advance && pos.equals( PREFIX ) ) ) {
                    // Reached the end of the bucket
                    break;
                }
                
                int comparison = key.compareTo( pos );
                
                if( advance && comparison < 0 ) {
                    // Reached the entry after key
                    break;
                } else if( !advance && comparison > 0 ) {
                    // Reached the entry before key
                    break;
                }
                    
                ++distance;
            }
            
            return distance;
        } catch (DatabaseException e) {
            throw DatabaseUtil.translateBDBException("Failed getting distance " + e.getMessage(), e);
        } finally {
            closeCursor(cursor);
        }
    }
    
    
}
