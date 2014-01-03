package com.amazon.messaging.seqstore.v3.bdb;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internal.StoredEntryV3;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketStorageType;
import com.amazon.messaging.seqstore.v3.store.BucketCursor;
import com.amazon.messaging.seqstore.v3.store.StorePosition;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.util.RuntimeExceptionWrapper;

/**
 * A bucket that is stored in a database shared by multiple buckets from multiple queues. Sharing the database
 * for multiple buckets makes the overhead per bucket much lower but makes deletion of the bucket more 
 * expensive as individual records have to be deleted instead of the entire database simply being dropped.
 * <p>
 * The key for entries in the shared database is the bucket's sequence id followed by the ackid for the entry. 
 * This uniquely identifies the entry and keeps it in the correct sorted order relative to other entries
 * in the same bucket.
 * <p>
 * Each bucket in the shared database has prefix and terminator record. These are used to ensure that when
 * iterating over a bucket cursors never lock any records not in that bucket. They are also used
 * for correctness checks to ensure that no records are returned from the wrong bucket and that 
 * when a bucket is loaded it with the correct id (the id is stored in the value for the prefix and terminator 
 * records).
 * 
 * @author stevenso
 *
 */
public class BDBSharedDBBucketStore extends BDBBackedBucketStore {
    private static final Log log = LogFactory.getLog(BDBSharedDBBucketStore.class);
    
    static final TupleBinding<BDBSharedDBBucketKey> KEY_BINDING = BDBSharedDBBucketKey.BINDING;
    
    static final StoredEntryV3EntryBinding ENTRY_BINDING = new StoredEntryV3EntryBinding();
    
    public static final AckIdV3 PREFIX = AckIdV3.MINIMUM;
    
    /**
     * An ack id that is sorted immediately after PREFIX and before any other possible records
     */
    static final AckIdV3 AFTER_PREFIX = new AckIdV3( AckIdV3.MINIMUM, true );
    
    static final AckIdV3 TERMINATOR = AckIdV3.MAXIMUM;
    
    private final Database commonDatabase;
    
    public static BDBSharedDBBucketStore createNewBucketStore(
        Transaction txn, Database commonDatabase,
        AckIdV3 bucketId, long bucketSequenceId, String bucketName )
        throws SeqStoreDatabaseException
    {
        BDBSharedDBBucketKey prefixKey = new BDBSharedDBBucketKey(bucketSequenceId, PREFIX);
        BDBSharedDBBucketKey terminatorKey = new BDBSharedDBBucketKey(bucketSequenceId, TERMINATOR);
        
        try {
            OperationStatus status;
            DatabaseEntry keyEntry = new DatabaseEntry();
            DatabaseEntry valueEntry = new DatabaseEntry();
            
            StoredEntry<AckIdV3> prefixEntry = new StoredEntryV3(PREFIX, new byte[0], getPrefixLogId(bucketName) );
            
            KEY_BINDING.objectToEntry(prefixKey, keyEntry);
            ENTRY_BINDING.objectToEntry(prefixEntry, valueEntry);
            status = commonDatabase.putNoOverwrite(txn, keyEntry, valueEntry);
            if( status == OperationStatus.KEYEXIST ) {
                throw new SeqStoreDatabaseException("Attempt to reuse bucket id " + bucketSequenceId );
            }
            DatabaseUtil.checkSuccess(status, "putNoOverwrite");
            
            StoredEntry<AckIdV3> terminatorEntry = new StoredEntryV3(TERMINATOR, new byte[0], getTerminatorLogId(bucketName) );
            
            KEY_BINDING.objectToEntry(terminatorKey, keyEntry);
            ENTRY_BINDING.objectToEntry(terminatorEntry, valueEntry);
            status = commonDatabase.putNoOverwrite(txn, keyEntry, valueEntry);
            if( status == OperationStatus.KEYEXIST ) {
                throw new SeqStoreDatabaseException("Attempt to reuse bucket id " + bucketSequenceId );
            }
            DatabaseUtil.checkSuccess(status, "putNoOverwrite");
        } catch (DatabaseException ex) {
            throw DatabaseUtil.translateBDBException( 
                    "Failed creating bucket " + bucketName + " (" + bucketSequenceId + ")", ex );
        }
        
        return new BDBSharedDBBucketStore( bucketId, bucketSequenceId, commonDatabase );
    }
    
    public static BDBSharedDBBucketStore loadBucketStore(
        Database commonDatabase, AckIdV3 bucketId, long bucketSequenceId,
        String bucketName ) 
            throws SeqStoreDatabaseException 
    {
        BDBSharedDBBucketKey prefixKey = new BDBSharedDBBucketKey(bucketSequenceId, PREFIX);
        BDBSharedDBBucketKey terminatorKey = new BDBSharedDBBucketKey(bucketSequenceId, TERMINATOR);
        
        try {
            OperationStatus status;
            StoredEntry<AckIdV3> entry;
            DatabaseEntry keyEntry = new DatabaseEntry();
            DatabaseEntry valueEntry = new DatabaseEntry();
            
            KEY_BINDING.objectToEntry(prefixKey, keyEntry);
            status = commonDatabase.get(null, keyEntry, valueEntry, LockMode.READ_COMMITTED );
            if( status == OperationStatus.NOTFOUND ) {
                throw new SeqStoreDatabaseException("Could not find bucket " + bucketName + "(" + bucketSequenceId + ")" );
            }
            DatabaseUtil.checkSuccess(status, "get");
            
            entry = ENTRY_BINDING.entryToObject(valueEntry);
            if( !entry.getLogId().equals(getPrefixLogId(bucketName)) ) {
                throw new SeqStoreDatabaseException(
                        "Name on bucket does not match the expected name. " +
                        "Expected prefix " + getPrefixLogId(bucketName) + " got " + entry.getLogId() );
            }
            
            KEY_BINDING.objectToEntry(terminatorKey, keyEntry);
            status = commonDatabase.get(null, keyEntry, valueEntry, LockMode.READ_COMMITTED );
            if( status == OperationStatus.NOTFOUND ) {
                throw new SeqStoreDatabaseException("Found prefix but not terminator for " + bucketName + "(" + bucketSequenceId + ")" );
            }
            DatabaseUtil.checkSuccess(status, "get");
            
            entry = ENTRY_BINDING.entryToObject(valueEntry);
            if( !entry.getLogId().equals(getTerminatorLogId(bucketName)) ) {
                throw new SeqStoreDatabaseException(
                        "Name on bucket does not match the expected name. " +
                        "Expected terminator " + getTerminatorLogId(bucketName) + " got " + entry.getLogId() );
            }
            
        } catch (DatabaseException ex) {
            throw DatabaseUtil.translateBDBException( "Failed loading bucket " + bucketName, ex );
        }
        
        return new BDBSharedDBBucketStore( bucketId, bucketSequenceId, commonDatabase );
    }
    
    public static void deleteBucketStore(Transaction txn, Database commonDatabase, String name, long bucketSequenceId )
            throws SeqStoreDatabaseException
    {
        BDBSharedDBBucketKey prefixKey = new BDBSharedDBBucketKey(bucketSequenceId, PREFIX);
        
        Cursor cursor = null;
        try {
            CursorConfig cursorConfig = new CursorConfig();
            cursorConfig.setReadCommitted(true);
            
            cursor = commonDatabase.openCursor(txn , cursorConfig);
            cursor.setCacheMode(CacheMode.EVICT_LN);
            
            OperationStatus status;
            DatabaseEntry keyEntry = new DatabaseEntry();
            DatabaseEntry valueEntry = new DatabaseEntry();
            
            KEY_BINDING.objectToEntry(prefixKey, keyEntry);
            status = cursor.getSearchKeyRange(keyEntry, valueEntry, LockMode.RMW);
            if( status == OperationStatus.NOTFOUND ) {
                // Log with a fake exception so there is stack trace showing how this was reached
                log.warn( "Bucket " + name + " with id " + bucketSequenceId + " was already deleted.", 
                        new RuntimeException("Already deleted") );
                return;
            }
            DatabaseUtil.checkSuccess(status, "getSearchKeyRange");
            
            BDBSharedDBBucketKey currentKey;
            currentKey = KEY_BINDING.entryToObject(keyEntry);
            if( currentKey.getBucketSequenceId() != bucketSequenceId ) {
                // Log with a fake exception so there is stack trace showing how this was reached
                log.warn( "Bucket " + name + " with id " + bucketSequenceId + " was already deleted.", 
                        new RuntimeException("Already deleted") );
                return;
            }
            
            StoredEntry<AckIdV3> entry = ENTRY_BINDING.entryToObject(valueEntry);
            if( !entry.getLogId().equals(getPrefixLogId(name)) ) {
                throw new SeqStoreDatabaseException(
                        "Name on bucket " + name + " does not match the expected name, or bucket " +
                		"was partially deleted. Expected header prefix " + getPrefixLogId(name) + 
                		" got " + entry.getLogId() );
            }
            
            valueEntry.setPartial(0, 0, true);
            do {
                status = cursor.delete();
                
                // The only allowed error is KEYEMPTY which can't happen as this transaction has already taken a write lock on
                //  the record
                DatabaseUtil.checkSuccess(status, "cursor.delete");
                
                status = cursor.getNextNoDup(keyEntry, valueEntry, LockMode.RMW);
                if( status == OperationStatus.NOTFOUND ) {
                    throw new SeqStoreDatabaseException( 
                            "Missing terminator for bucket " + name + "(" + bucketSequenceId + ")" );
                }
                
                DatabaseUtil.checkSuccess(status, "cursor.next");
                
                currentKey = KEY_BINDING.entryToObject(keyEntry);
                
                if( currentKey.getBucketSequenceId() != bucketSequenceId ) {
                    throw new SeqStoreDatabaseException( 
                            "Missing terminator for bucket " + name + "(" + bucketSequenceId + ")" );
                }
            } while( !currentKey.getAckId().equals( TERMINATOR ) );
            
            // Delete the terminator
            status = cursor.delete();
            DatabaseUtil.checkSuccess(status, "cursor.delete");
        } catch (DatabaseException ex) {
            throw DatabaseUtil.translateBDBException( 
                    "Failed deleting bucket " + name + " (" + bucketSequenceId + ")", ex );
        } finally {
            if( cursor == null ) return;
            
            try {
                cursor.close();
            } catch( DatabaseException e ){
                log.error( "Error closing cursor: ", e );
                if( !commonDatabase.getEnvironment().isValid() ) {
                    // If the environment is invalid don't hide the place where it became invalid
                    throw DatabaseUtil.translateBDBException(e);
                }
            }
        }
    }
    
    @Override
    public BucketStorageType getBucketStorageType() {
        return BucketStorageType.SharedDatabase;
    }

    private static String getTerminatorLogId(String name) {
        return "*TERMINATOR-" + name + "*";
    }

    private static String getPrefixLogId(String name) {
        return "*PREFIX-" + name + "*";
    }
    
    private BDBSharedDBBucketStore(AckIdV3 bucketId, long bucketSequenceId, Database commonDatabase ) {
        super( bucketId, bucketSequenceId );
        this.commonDatabase = commonDatabase;
    }

    @Override
    public BucketCursor createCursor() throws SeqStoreDatabaseException {
        return new BDBSharedDBBucketCursor(commonDatabase, sequenceId);
    }

    /**
     * Store the key and value.<br>
     * 
     * This implementation ignores the value of cache as deletion of the record would require rereading the LN if it 
     * was not in cache. The documentation for BDB JE 5 suggests that is no longer that case for BDB JE 5 so this 
     * should be reconsidered when we upgrade.
     */
    @Override
    public void put(AckIdV3 key, StoredEntry<AckIdV3> value, boolean cache) throws SeqStoreDatabaseException {
        BDBSharedDBBucketKey sharedDBKey = new BDBSharedDBBucketKey(sequenceId, key);
        try {
            DatabaseEntry keyEntry = new DatabaseEntry();
            DatabaseEntry valueEntry = new DatabaseEntry();
            KEY_BINDING.objectToEntry(sharedDBKey, keyEntry);
            ENTRY_BINDING.objectToEntry(value, valueEntry);
            
            OperationStatus result;
            if( !cache ) {
                Transaction txn = commonDatabase.getEnvironment().beginTransaction(
                        null, null);
                try {
                    Cursor cursor = commonDatabase.openCursor(txn, null);
                    try {
                        cursor.put(keyEntry, valueEntry);
                    } finally {
                        cursor.close();
                    }
                    txn.commit();
                } finally {
                    txn.abort(); // Will abort the transaction if not already commited
                }
            } else {
                result = commonDatabase.put(null, keyEntry, valueEntry);
                
                DatabaseUtil.checkSuccess(result, "put");
            }
        } catch (DatabaseException ex) {
            throw DatabaseUtil.translateBDBException( "Failed inserting into database", ex );
        }
    }

    @Override
    public StoredEntry<AckIdV3> get(AckIdV3 key) throws SeqStoreDatabaseException {
        BDBSharedDBBucketKey sharedDBKey = new BDBSharedDBBucketKey(sequenceId, key);
        try {
            DatabaseEntry keyEntry = new DatabaseEntry();
            DatabaseEntry valueEntry = new DatabaseEntry();
            KEY_BINDING.objectToEntry(sharedDBKey, keyEntry);
            
            // READ_UNCOMMITTED is fine as we don't promise that there are no phantom reads of 
            // uncompleted enqueues, and if the bucket is being deleted the shouldn't be 
            // any references to it.
            OperationStatus result = 
                commonDatabase.get(null, keyEntry, valueEntry, LockMode.READ_UNCOMMITTED);
            if( result == OperationStatus.NOTFOUND ) {
                return null;
            } 
            
            DatabaseUtil.checkSuccess(result, "get");
            
            return ENTRY_BINDING.entryToObject(valueEntry);
        } catch ( DatabaseException ex ) {
            throw DatabaseUtil.translateBDBException( "Failed fetching item from database", ex );
        }
    }

    @Override
    public AckIdV3 getFirstId() throws SeqStoreDatabaseException {
        try {
            CursorConfig cursorConfig = new CursorConfig();
            cursorConfig.setReadUncommitted(true);
            Cursor cursor = commonDatabase.openCursor(null, cursorConfig);
            try {
                cursor.setCacheMode(CacheMode.UNCHANGED);
                DatabaseEntry keyEntry = new DatabaseEntry();
                DatabaseEntry valueEntry = new DatabaseEntry();
                valueEntry.setPartial(0, 0, true);
                KEY_BINDING.objectToEntry( new BDBSharedDBBucketKey(sequenceId, AFTER_PREFIX), keyEntry );
                
                OperationStatus status = cursor.getSearchKeyRange(keyEntry, valueEntry, null);
                if( status == OperationStatus.NOTFOUND ) {
                    throwMissingBoundryError(true);
                }
                DatabaseUtil.checkSuccess(status, "getSearchKeyRange");
                
                BDBSharedDBBucketKey sharedDBKey = getKeyFromResult(keyEntry, true); 
                if( sharedDBKey.getAckId().equals( TERMINATOR ) ) return null;
                return sharedDBKey.getAckId();
            } finally {
                cursor.close();
            }
        } catch ( DatabaseException ex ) {
            throw DatabaseUtil.translateBDBException( "Failed fetching first id from database", ex );
        }
    }

    @Override
    public AckIdV3 getLastId() throws SeqStoreDatabaseException {
        try {
            CursorConfig cursorConfig = new CursorConfig();
            cursorConfig.setReadUncommitted(true);
            Cursor cursor = commonDatabase.openCursor(null, cursorConfig);
            try {
                cursor.setCacheMode(CacheMode.UNCHANGED);
                DatabaseEntry keyEntry = new DatabaseEntry();
                DatabaseEntry valueEntry = new DatabaseEntry();
                valueEntry.setPartial(0, 0, true);
                
                KEY_BINDING.objectToEntry( new BDBSharedDBBucketKey(sequenceId, TERMINATOR), keyEntry );
                OperationStatus status = cursor.getSearchKey(keyEntry, valueEntry, null);
                if( status == OperationStatus.NOTFOUND ) {
                    throwMissingBoundryError(true);
                }
                DatabaseUtil.checkSuccess(status, "getSearchKey");
                
                status = cursor.getPrev(keyEntry, valueEntry, null);
                if( status == OperationStatus.NOTFOUND ) {
                    throwMissingBoundryError(false);
                }
                DatabaseUtil.checkSuccess(status, "getPrev");
                
                BDBSharedDBBucketKey sharedDBKey = getKeyFromResult(keyEntry, false);
                
                if( sharedDBKey.getAckId().equals( PREFIX ) ) {
                    return null;
                }
                
                return sharedDBKey.getAckId();
            } finally {
                cursor.close();
            }
        } catch ( DatabaseException ex ) {
            throw DatabaseUtil.translateBDBException( "Failed fetching first id from database", ex );
        }
    }

    @Override
    public long getCount() throws SeqStoreDatabaseException {
        long count = 0;
        try {
            CursorConfig cursorConfig = new CursorConfig();
            cursorConfig.setReadUncommitted(true);
            Cursor cursor = commonDatabase.openCursor(null, cursorConfig);
            try {
                cursor.setCacheMode(CacheMode.UNCHANGED);
                DatabaseEntry keyEntry = new DatabaseEntry();
                DatabaseEntry valueEntry = new DatabaseEntry();
                valueEntry.setPartial(0, 0, true);
                
                KEY_BINDING.objectToEntry( new BDBSharedDBBucketKey(sequenceId, AFTER_PREFIX ), keyEntry );
                
                OperationStatus status = cursor.getSearchKeyRange(keyEntry, valueEntry, null);
                if( status == OperationStatus.NOTFOUND ) {
                    throwMissingBoundryError(true);
                }
                DatabaseUtil.checkSuccess(status, "getSearchKeyRange");
                
                BDBSharedDBBucketKey sharedDBKey = getKeyFromResult(keyEntry, true);
                while( !sharedDBKey.getAckId().equals( TERMINATOR ) ) {
                    count++;
                    
                    status = cursor.getNextNoDup(keyEntry, valueEntry, null);
                    if( status == OperationStatus.NOTFOUND ) {
                        throwMissingBoundryError(true);
                    }
                   
                    DatabaseUtil.checkSuccess(status, "getCount");
                    
                    sharedDBKey = getKeyFromResult(keyEntry, true);
                }
                
                return count;
            } finally {
                cursor.close();
            }
        } catch ( DatabaseException ex ) {
            throw DatabaseUtil.translateBDBException( "Failed fetching first id from database", ex );
        }
    }
    
    /**
     * This function is to get the StorePosition if an ackid is provided that already has an offset. This function
     * may fail if the ackid specified doesn't actually exist in the bucket. That is always an error but one that
     * has been seen to happen before so the existence of the record is always checked
     * 
     * @return the position for fullAckId in the bucket, or null if there is no entry matching the coreAckId for fullAckId.
     */
    private StorePosition getPositionGivenBucketOffset(AckIdV3 fullAckId) 
            throws SeqStoreDatabaseException 
    {
        BDBSharedDBBucketKey sharedDBKey = new BDBSharedDBBucketKey(sequenceId, fullAckId.getCoreAckId());
        try {
            AckIdV3 coreAckId = fullAckId.getCoreAckId();
            DatabaseEntry ackIdEntry = new DatabaseEntry();
            KEY_BINDING.objectToEntry(sharedDBKey, ackIdEntry);
            
            DatabaseEntry keyEntry = new DatabaseEntry();
            keyEntry.setData( ackIdEntry.getData() );
            
            DatabaseEntry valueEntry = new DatabaseEntry();
            valueEntry.setPartial(0, 0, true);
            
            Cursor cursor = commonDatabase.openCursor(null, CursorConfig.READ_UNCOMMITTED );
            try {
                cursor.setCacheMode(CacheMode.UNCHANGED);
                
                OperationStatus result = cursor.getSearchKey(keyEntry, valueEntry, null);
                if( result == OperationStatus.NOTFOUND ) {
                    log.warn( 
                        "Could not find entry " + fullAckId + " in " + getNameForLogging() + " even though a position was provided.");
                    return null;
                }
                
                DatabaseUtil.checkSuccess(result, "getSearchKey");

                BDBSharedDBBucketKey positionKey;
                
                if( fullAckId.isInclusive() != null && !fullAckId.isInclusive() ) {
                    // If isInclusive is false we need to return the position of the key before fullAckId so move the cursor
                    //  back one and use that key
                    result = cursor.getPrevNoDup(keyEntry, valueEntry, null);
                    if( result == OperationStatus.NOTFOUND ) {
                        throwMissingBoundryError(false);
                    }
                    
                    DatabaseUtil.checkSuccess(result, "getPrevNoDup");
                    
                    positionKey = getKeyFromResult(keyEntry, false);
                        
                    if( positionKey.getAckId().equals( PREFIX ) ) {
                        if( fullAckId.getBucketPosition() != 1 ) {
                            log.warn( 
                                    fullAckId + " claims not to be the first entry but no previous entries could be found.");
                        }
                        // Even if the position provided by fullAckId is clearly wrong we do know the correct position to return 
                        return new StorePosition(bucketId, null);
                    }
                    
                    return new StorePosition(bucketId, new AckIdV3( positionKey.getAckId(), fullAckId.getBucketPosition() - 1 ) );
                } else {
                    // We can skip deserializing the key as it wouldn't have been returned if it's ackid didn't match coreAckId
                    return new StorePosition(bucketId, new AckIdV3( coreAckId, fullAckId.getBucketPosition() ) );
                }
            } finally {
                cursor.close();
            }
        } catch ( DatabaseException ex ) {
            throw DatabaseUtil.translateBDBException( "Failed fetching item from database", ex );
        }
    }

    @Override
    public StorePosition getPosition(AckIdV3 fullAckId) throws SeqStoreDatabaseException {
        if( fullAckId.getBucketPosition() != null ) {
            StorePosition retval = getPositionGivenBucketOffset(fullAckId);
            if( retval != null ) return retval;
        }
        
        // If there is no provided position or the matching id can't be found so try it the hard way by iterating
        //  over all entries in the bucket until an exact match is found or the cursor goes to far
        BDBSharedDBBucketKey sharedDBKey = new BDBSharedDBBucketKey(sequenceId, fullAckId.getCoreAckId());
        try {
            DatabaseEntry keyEntry = new DatabaseEntry();
            DatabaseEntry valueEntry = new DatabaseEntry();
            valueEntry.setPartial(0, 0, true);
            
            DatabaseEntry ackIdEntry = new DatabaseEntry();
            KEY_BINDING.objectToEntry(sharedDBKey, ackIdEntry);
            
            Cursor cursor = commonDatabase.openCursor(null, CursorConfig.READ_UNCOMMITTED );
            try {
                cursor.setCacheMode(CacheMode.UNCHANGED);
                
                boolean first = true;
                byte[] previousKeyData;
                int keyComparison = 0;
                long pos = 0;
                do {
                    // Note that getNextNoDup always uses a new array to return the result so we can safely keep a reference to 
                    //  the old one
                    previousKeyData = keyEntry.getData();
                    
                    OperationStatus result;
                    if( first ) {
                        // Start the cursor off immediately after the prefix
                        KEY_BINDING.objectToEntry(new BDBSharedDBBucketKey(sequenceId, AFTER_PREFIX), keyEntry);
                        result = cursor.getSearchKeyRange(keyEntry, valueEntry, null );
                        first = false;
                    } else {
                        result = cursor.getNextNoDup(keyEntry, valueEntry, null );
                    }
                    
                    if( result == OperationStatus.NOTFOUND ) {
                        throwMissingBoundryError(true);
                    }
                    
                    DatabaseUtil.checkSuccess(result, "getSearchKeyRange|getNextNoDup" );
                    pos++;
                    
                    keyComparison = DatabaseUtil.compareKeys( keyEntry, ackIdEntry );
                } while( keyComparison < 0 ); // Loop until an exact match or the cursor moved to far
                
                boolean reachedEnd = false;
                if( keyComparison > 0 ) { 
                    // The cursor moved after the record we want so its possible it reached the end of the 
                    //  bucket
                    BDBSharedDBBucketKey lastKeyReached = KEY_BINDING.entryToObject( keyEntry );
                    if( lastKeyReached.getBucketSequenceId() != sequenceId ) {
                        throwMissingBoundryError(true);
                    }
                    if( lastKeyReached.getAckId().equals( TERMINATOR ) ) {
                        pos--;
                        reachedEnd = true;
                    }
                }
                
                if( reachedEnd ) {
                    // There is nothing >= to fulLAckId
                    AckIdV3 ackId;
                    if( previousKeyData == null ) {
                        assert pos == 0;
                        ackId = null;
                    } else {
                        BDBSharedDBBucketKey finalKey = KEY_BINDING.entryToObject(new TupleInput(previousKeyData));
                        ackId = new AckIdV3( finalKey.getAckId(), pos );
                    }
                    return new StorePosition(bucketId, ackId);
                } else if( keyComparison == 0 ) {
                    // Got exact match with core of fullAckId
                    if( fullAckId.isInclusive() != null && !fullAckId.isInclusive() ) {
                        // If isInclusive is false we want the one before the exact match
                        AckIdV3 ackId;
                        if( previousKeyData == null ) {
                            ackId = null;
                        } else {
                            BDBSharedDBBucketKey finalKey = KEY_BINDING.entryToObject(new TupleInput(previousKeyData));
                            ackId = new AckIdV3( finalKey.getAckId(), pos - 1);
                        }
                        return new StorePosition(bucketId, ackId );
                    } else {
                        // If isInclusive is null we want the exact match and if its true we still know
                        //  that there are no possible ackIds greater than the exact match but less
                        //  fullAckId
                        BDBSharedDBBucketKey finalKey = KEY_BINDING.entryToObject(keyEntry);
                        AckIdV3 ackId = new AckIdV3( finalKey.getAckId(), pos );
                        return new StorePosition(bucketId, ackId); 
                    }
                } else {
                    // There is no exact match so return the first one before fullAckId
                    AckIdV3 ackId;
                    if( previousKeyData == null ) {
                        ackId = null;
                    } else {
                        BDBSharedDBBucketKey finalKey = KEY_BINDING.entryToObject(new TupleInput(previousKeyData));
                        ackId = new AckIdV3( finalKey.getAckId(), pos - 1);
                    }
                    return new StorePosition( bucketId, ackId );
                }
            } finally {
                cursor.close();
            }
        } catch ( DatabaseException ex ) {
            throw DatabaseUtil.translateBDBException( "Failed fetching item from database", ex );
        } catch ( RuntimeExceptionWrapper ex ) {
            throw DatabaseUtil.translateWrappedBDBException( "Failed fetching item from database", ex );
        }
    }
    
    private void throwMissingBoundryError( boolean advancing ) throws SeqStoreDatabaseException {
        if( advancing ) {
            throw new SeqStoreDatabaseException( 
                    "Missing terminator for bucket " + getNameForLogging() + "(" + sequenceId + ")" );
        } else {
            throw new SeqStoreDatabaseException( 
                    "Missing prefix for bucket " + getNameForLogging() + "(" + sequenceId + ")" );
        }
    }
    
    private BDBSharedDBBucketKey getKeyFromResult( DatabaseEntry keyDBEntry, boolean advancing )
            throws SeqStoreDatabaseException
    {
        BDBSharedDBBucketKey key = KEY_BINDING.entryToObject(keyDBEntry);
        if( key.getBucketSequenceId() != sequenceId ) {
           throwMissingBoundryError(advancing);
        }
        
        return key;
    }

    @Override
    public void close() throws SeqStoreDatabaseException {
        // Nothing to do
    }
}
