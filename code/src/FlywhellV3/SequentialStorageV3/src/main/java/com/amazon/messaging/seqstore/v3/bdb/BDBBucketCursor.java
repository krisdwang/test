package com.amazon.messaging.seqstore.v3.bdb;

import lombok.Getter;
import lombok.Setter;

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
import com.sleepycat.je.Transaction;


public final class BDBBucketCursor implements BucketCursor {
    private static final AckIdV3EntryBinding keyBinder_ = new AckIdV3EntryBinding();

    private static final StoredEntryV3EntryBinding entryBinder_ = new StoredEntryV3EntryBinding();
    
    private static final boolean closeCursor = false;
    
    private final Database db;
    
    private final Transaction txn;
    
    private Cursor cursor;
    
    private boolean closed = false;
    
    private AckIdV3 current;
    
    @Getter @Setter
    private boolean evictFromCacheAfterReading;
    
    BDBBucketCursor(Database db, Transaction txn) {
        this.db = db;
        this.txn = txn;
    }
    
    BDBBucketCursor(BDBBucketCursor source) throws SeqStoreDatabaseException {
        this.db = source.db;
        this.txn = source.txn;
        this.current = source.current;
        this.evictFromCacheAfterReading = source.evictFromCacheAfterReading;
        
        if (source.cursor == null) {
            cursor = null;
        } else {
            try {
                cursor = source.cursor.dup(true);
            } catch (DatabaseException e) {
                throw DatabaseUtil.translateBDBException("Failed duplicating cursor: " + e.getMessage(), e);
            } catch( IllegalStateException e ) {
                throw new SeqStoreDatabaseException("Failed duplicating cursor: " + e.getMessage(), e);
            }
        }
    }

    @Override
    public void close() throws SeqStoreDatabaseException {
        closeCursor();
        closed = true;
    }
    
    @Override
    public BucketCursor copy() throws SeqStoreDatabaseException {
        checkNotClosed();
        
        return new BDBBucketCursor( this );
    }

    @Override
    public AckIdV3 current() {
        return current;
    }
    
    @Override
    public StoredEntry<AckIdV3> nextEntry(AckIdV3 max) 
            throws SeqStoreDatabaseException 
    {
        checkNotClosed();
        
        DatabaseEntry keyDBEntry = new DatabaseEntry();
        DatabaseEntry valueDBEntry = new DatabaseEntry();
        
        CacheMode cacheMode;
        if( evictFromCacheAfterReading ) {
            cacheMode = CacheMode.EVICT_LN;
        } else {
            cacheMode = CacheMode.DEFAULT;
        }
        
        try {
            AckIdV3 key = next( max, keyDBEntry, valueDBEntry, cacheMode );
            
            if( key == null ) return null;
            return entryBinder_.entryToObject(valueDBEntry);
        } finally {
            if( closeCursor ) closeCursor();
        }
    }
    
    @Override
    public long advanceToKey(AckIdV3 key) throws SeqStoreDatabaseException {
        checkNotClosed();
        
        DatabaseEntry keyDBEntry = new DatabaseEntry();
        DatabaseEntry valueDBEntry = new DatabaseEntry();
        valueDBEntry.setPartial(0, 0, true );
        
        CacheMode cacheMode;
        if( evictFromCacheAfterReading ) {
            cacheMode = CacheMode.EVICT_LN;
        } else {
            cacheMode = CacheMode.UNCHANGED;
        }
        
        try {
            long count = 0;
            while( next( key, keyDBEntry, valueDBEntry, cacheMode ) != null ) count++;
            return count;
        } finally {
            if( closeCursor ) closeCursor();
        }
        
    }
    
    @Override
    public boolean jumpToKey(AckIdV3 key) throws SeqStoreDatabaseException {
        checkNotClosed();
        
        DatabaseEntry keyDBEntry = new DatabaseEntry();
        DatabaseEntry valueDBEntry = new DatabaseEntry();
        valueDBEntry.setPartial(0, 0, true );
        
        try {
            if (cursor == null) {
                createCursor();
            }
            
            CacheMode cacheMode;
            if( evictFromCacheAfterReading ) {
                cacheMode = CacheMode.EVICT_LN;
            } else {
                cacheMode = CacheMode.UNCHANGED;
            }
            cursor.setCacheMode(cacheMode);
            
            AckIdV3 coreAckId = key.getCoreAckId();

            keyBinder_.objectToEntry(coreAckId, keyDBEntry);
            OperationStatus status = cursor.getSearchKeyRange(keyDBEntry, valueDBEntry, null);

            if( status == OperationStatus.NOTFOUND ) {
                // Nothing equal to or after the specified key, so start at the end of the database
                status = cursor.getLast(keyDBEntry, valueDBEntry, null);
                
                if( status == OperationStatus.NOTFOUND ) {
                    // Empty database
                    assert current == null;
                    closeCursor();
                    return false;
                }
            }

            if (status != OperationStatus.SUCCESS) {
                throw new SeqStoreDatabaseException( "Unexpected result from BDB cursor: " + status );
            }

            AckIdV3 cursorKey = keyBinder_.entryToObject(keyDBEntry);
            boolean foundExactKey = cursorKey.equals( coreAckId );
            
            if( !foundExactKey || ( key.isInclusive() != null && !key.isInclusive() ) ) {
                cursorKey = backupToBeforeKey(key, keyDBEntry, valueDBEntry, cursorKey);
            }
            
            current = cursorKey;
            
            return foundExactKey;
        } catch (DatabaseException ex) {
            throw DatabaseUtil.translateBDBException("Failed advancing cursor:" + ex.getMessage(), ex);
        } finally {
            if( closeCursor ) closeCursor();
        }
    }
    
    
    private AckIdV3 backupToBeforeKey(AckIdV3 key, DatabaseEntry keyDBEntry, DatabaseEntry valueDBEntry,
                                      AckIdV3 cursorKey) throws DatabaseException, SeqStoreDatabaseException 
    {
        OperationStatus status;

        while( key.compareTo(cursorKey) < 0 ) {
            status = cursor.getPrevNoDup(keyDBEntry, valueDBEntry, null);
            if( status == OperationStatus.NOTFOUND ) {
                cursorKey = null;
                break;
            } else if( status == OperationStatus.SUCCESS ) {
                cursorKey = keyBinder_.entryToObject(keyDBEntry);
            } else {
                throw new SeqStoreDatabaseException( "Unexpected result from BDB cursor: " + status );
            }
        }

        return cursorKey;
    }

    private AckIdV3 next(AckIdV3 max, DatabaseEntry keyDBEntry, DatabaseEntry valueDBEntry, CacheMode cacheMode) throws SeqStoreDatabaseException {
        OperationStatus status;

        try {
            if (cursor == null) {
                createCursor();
                cursor.setCacheMode(cacheMode);
                if (current != null) {
                    keyBinder_.objectToEntry(new AckIdV3(current, true), keyDBEntry);
                    status = cursor.getSearchKeyRange(keyDBEntry, valueDBEntry, null);
                } else {
                    status = cursor.getFirst(keyDBEntry, valueDBEntry, null);
                }
            } else {
                cursor.setCacheMode(cacheMode);
                status = cursor.getNext(keyDBEntry, valueDBEntry, null);
            }
            
            if (status != OperationStatus.SUCCESS) {
                if( status != OperationStatus.NOTFOUND ) {
                    throw new SeqStoreDatabaseException( "Unexpected result from BDB cursor: " + status );
                }
                return null;
            }

            AckIdV3 newCurrent = keyBinder_.entryToObject(keyDBEntry);
            if (max != null && newCurrent.compareTo(max) > 0) {
                if( current == null ) {
                    closeCursor();
                } else {
                    backup(keyDBEntry, valueDBEntry, current); // Backup to the old position
                }

                // At the time of getNext there were no records between the
                // current position and max so return null
                return null;
            }

            current = newCurrent;
            return newCurrent;
        } catch (DatabaseException ex) {
            throw DatabaseUtil.translateBDBException("Failed advancing cursor:" + ex.getMessage(), ex);
        }
    }
    
    private void createCursor() throws SeqStoreDatabaseException {
        CursorConfig config = new CursorConfig();
        config.setReadUncommitted(true);
        try {
            cursor = db.openCursor(txn, config);
        } catch( DatabaseException e ) {
            throw DatabaseUtil.translateBDBException("Failed opening cursor: " + e.getMessage(), e);
        } catch( IllegalStateException e ) {
            throw new SeqStoreDatabaseException("Error opening cursor: " + e.getMessage(), e );
        }
    }
    
    private void backup(DatabaseEntry keyDB, DatabaseEntry data, AckIdV3 to) throws DatabaseException {
        AckIdV3 newCurrent;
        do {
            OperationStatus status = cursor.getPrev(keyDB, data, null);
            if (status != OperationStatus.SUCCESS) {
                throw new AssertionError("Could not find record that we were just on.");
            }

            newCurrent = keyBinder_.entryToObject(keyDB);
        } while (!newCurrent.equals(to));
    }
    
    private void closeCursor() throws SeqStoreDatabaseException {
        if (cursor != null) {
            try {
                cursor.close();
            } catch (DatabaseException e) {
                throw DatabaseUtil.translateBDBException("Failed closing cursor: " + e.getMessage(), e);
            }
            cursor = null;
        }
    }

    private void checkNotClosed() {
        if( closed ) throw new IllegalStateException( "Attempt to use closed cursor.");
    }
    
    @Override
    public long getDistance(AckIdV3 key) throws SeqStoreDatabaseException {
        checkNotClosed();
        
        if( key == current ) return 0;
        if( key == null ) key = AckIdV3.MINIMUM;
        else if( key.equals( current ) ) return 0;
        
        DatabaseEntry keyDBEntry = new DatabaseEntry();
        DatabaseEntry valueDBEntry = new DatabaseEntry();
        valueDBEntry.setPartial(0, 0, true );
        
        Cursor countCursor = null;
        try {
            if( cursor != null ) {
                countCursor = cursor.dup(true);
                countCursor.setCacheMode( CacheMode.UNCHANGED );
            } else {
                CursorConfig config = new CursorConfig();
                config.setReadUncommitted(true);
                countCursor = db.openCursor(txn, config);
                countCursor.setCacheMode( CacheMode.UNCHANGED );
                
                if( current != null ) {
                    keyBinder_.objectToEntry(current.getCoreAckId(), keyDBEntry);
                    OperationStatus status = countCursor.getSearchKey(keyDBEntry, valueDBEntry, null);
                    
                    if( status == OperationStatus.NOTFOUND ) {
                        throw new SeqStoreDatabaseException("Unable to seek to current location" );
                    } else if( status != OperationStatus.SUCCESS ) {
                        throw new SeqStoreDatabaseException( "Unexpected result from BDB cursor: " + status );
                    }
                }
            }
            
            boolean advance;
            if( current == null ) advance = true;
            else advance = key.compareTo( current ) > 0;
            
            long distance = 0;
            for(;;) {
                OperationStatus status;
                if( advance ) {
                    status = countCursor.getNextNoDup(keyDBEntry, valueDBEntry, null);
                } else {
                    status = countCursor.getPrevNoDup(keyDBEntry, valueDBEntry, null);
                }
                
                if( status == OperationStatus.SUCCESS ) {
                    AckIdV3 pos = keyBinder_.entryToObject(keyDBEntry);
                    int comparison = key.compareTo( pos );
                    
                    if( advance && comparison < 0 ) {
                        // Reached the entry after key
                        break;
                    } else if( !advance && comparison > 0 ) {
                        // Reached the entry before key
                        break;
                    }
                    
                    ++distance;
                    
                    if( comparison == 0 ) {
                        // We're exactly on the specified key.
                        break;
                    }
                } else if( status == OperationStatus.NOTFOUND ) {
                    // Reached the start/end of the bucket
                    break;
                } else {
                    throw new SeqStoreDatabaseException( "Unexpected result from BDB cursor: " + status );
                }
            }
            
            return distance;
        } catch (DatabaseException e) {
            throw DatabaseUtil.translateBDBException("Failed getting distance " + e.getMessage(), e);
        } finally {
            if( countCursor != null ) {
                try {
                    countCursor.close();
                } catch (DatabaseException e) {
                    throw DatabaseUtil.translateBDBException("Failed closing cursor " + e.getMessage(), e);
                }
            }
        }
    }
}
