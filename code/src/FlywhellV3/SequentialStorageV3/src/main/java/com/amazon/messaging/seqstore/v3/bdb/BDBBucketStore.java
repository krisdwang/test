package com.amazon.messaging.seqstore.v3.bdb;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.messaging.seqstore.v3.StoredEntry;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.internal.AckIdV3;
import com.amazon.messaging.seqstore.v3.internalInterface.BucketStorageType;
import com.amazon.messaging.seqstore.v3.store.BucketCursor;
import com.amazon.messaging.seqstore.v3.store.BucketStore;
import com.amazon.messaging.seqstore.v3.store.StorePosition;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.util.RuntimeExceptionWrapper;

public class BDBBucketStore extends BDBBackedBucketStore {
    private static final Log log = LogFactory.getLog(BDBBucketStore.class);
    
    private static final AckIdV3EntryBinding KEY_BINDING = new AckIdV3EntryBinding();
    
    private static final StoredEntryV3EntryBinding ENTRY_BINDING = new StoredEntryV3EntryBinding();
    
    private final Database database;
    
    // Read only transaction for the bucket
    private final Transaction txn_;
    
    private final BucketStorageType bucketStorageType;
    
    public static BDBBucketStore getOrCreateBucketStore(
        Environment env, Transaction txn, 
        BucketStorageType bucketStorageType, AckIdV3 bucketId, long bucketSequenceId,
        String bucketName, boolean readOnly, boolean newBucket ) 
                throws SeqStoreDatabaseException
    {
        Database db;
        String databaseName;
        if( bucketSequenceId == BucketStore.UNASSIGNED_SEQUENCE_ID ) {
            databaseName = bucketName;
        } else {
            databaseName = makeShortName(bucketSequenceId);
        }
        
        try {
            db = DatabaseUtil.createDB(env, txn, databaseName, newBucket, readOnly );
        } catch( DatabaseException e ) {
            throw DatabaseUtil.translateBDBException(e);
        }
        
        return new BDBBucketStore( bucketStorageType, bucketName, bucketId, bucketSequenceId, db );
    }

    private static String makeShortName(long bucketSequenceId) {
        return "B#" + bucketSequenceId;
    }
    
    public static void deleteBucketStore( 
        Environment env, Transaction txn,
        String name, long bucketSequenceId )
            throws SeqStoreDatabaseException
    {
        String databaseName;
        if( bucketSequenceId == BucketStore.UNASSIGNED_SEQUENCE_ID ) {
            databaseName = name;
        } else {
            databaseName = makeShortName(bucketSequenceId);
        }
        
        try {
            env.removeDatabase(txn, databaseName);
        } catch( DatabaseNotFoundException e ) {
            log.warn( "Bucket " + name + " with database name " + databaseName + " was already deleted.", e );
        } catch( DatabaseException e ) {
            throw DatabaseUtil.translateBDBException(e);
        }
    }
    
    private BDBBucketStore(
        BucketStorageType bucketStorageType, String bucketName, AckIdV3 bucketId, long bucketSequenceId, Database database)
            throws SeqStoreDatabaseException
    {
        super( bucketId, bucketSequenceId );
        if( bucketStorageType != BucketStorageType.DedicatedDatabase && bucketStorageType != BucketStorageType.DedicatedDatabaseInLLMEnv ) {
            throw new IllegalStateException( "Invalid bucket type for " + this.getClass().getName() + ": " + bucketStorageType ); 
        }
        
        this.bucketStorageType = bucketStorageType;
        this.database = database;
        TransactionConfig txnConfig = new TransactionConfig();
        txnConfig.setReadUncommitted(true);
        try {
            txn_ = database.getEnvironment().beginTransaction(null, txnConfig);
            String txnName;
            if( bucketSequenceId != BucketStore.UNASSIGNED_SEQUENCE_ID ) {
                txnName = "B:" + bucketSequenceId;
            } else {
                txnName = "B:" + bucketName;
            }
            txn_.setName("BDBBucket-" + txnName );
        } catch (DatabaseException e) {
            throw DatabaseUtil.translateBDBException("Cant start a transaction", e);
        }
    }
    
    @Override
    public BucketStorageType getBucketStorageType() {
        return bucketStorageType;
    }
    
    @Override
    public StoredEntry<AckIdV3> get(AckIdV3 key) throws SeqStoreDatabaseException {
        try {
            DatabaseEntry keyEntry = new DatabaseEntry();
            DatabaseEntry valueEntry = new DatabaseEntry();
            KEY_BINDING.objectToEntry(key, keyEntry);
            OperationStatus result = 
                database.get(null, keyEntry, valueEntry, LockMode.READ_UNCOMMITTED);
            if( result == OperationStatus.NOTFOUND ) {
                return null;
            } 
            
            DatabaseUtil.checkSuccess(result, "get");
            
            return ENTRY_BINDING.entryToObject(valueEntry);
        } catch ( DatabaseException ex ) {
            throw DatabaseUtil.translateBDBException( "Failed fetching item from database", ex );
        }
    }
    
    private StorePosition getPositionGivenBucketOffset(AckIdV3 fullAckId) 
            throws SeqStoreDatabaseException 
    {
        try {
            AckIdV3 coreAckId = fullAckId.getCoreAckId();
            DatabaseEntry ackIdEntry = new DatabaseEntry();
            KEY_BINDING.objectToEntry(coreAckId, ackIdEntry);
            
            DatabaseEntry keyEntry = new DatabaseEntry();
            keyEntry.setData( ackIdEntry.getData() );
            
            DatabaseEntry valueEntry = new DatabaseEntry();
            valueEntry.setPartial(0, 0, true);
            
            Cursor cursor = database.openCursor(txn_, CursorConfig.READ_UNCOMMITTED );
            try {
                cursor.setCacheMode(CacheMode.UNCHANGED);
                
                OperationStatus result = cursor.getSearchKey(keyEntry, valueEntry, null);
                if( result == OperationStatus.NOTFOUND ) {
                    log.warn( 
                        "Could not find entry " + fullAckId + " in " + getNameForLogging() + " even though a position was provided.");
                    return null;
                }
                
                DatabaseUtil.checkSuccess(result, "getSearchKey");

                boolean getPrevious = fullAckId.isInclusive() != null && !fullAckId.isInclusive() ;
                if( getPrevious ) {
                    result = cursor.getPrevNoDup(keyEntry, valueEntry, null);
                    if( result == OperationStatus.NOTFOUND ) {
                        if( fullAckId.getBucketPosition() != 1 ) {
                            log.warn( 
                                    fullAckId + " claims not to be the first entry but no previous entries could be found.");
                        }
                        return new StorePosition(bucketId, null);
                    }
                    
                    DatabaseUtil.checkSuccess(result, "getPrevNoDup");
                }
                
                AckIdV3 positionKey = KEY_BINDING.entryToObject(keyEntry);
                if( getPrevious ) {
                    return new StorePosition(bucketId, new AckIdV3( positionKey, fullAckId.getBucketPosition() - 1 ) );
                } else {
                    return new StorePosition(bucketId, new AckIdV3( positionKey, fullAckId.getBucketPosition() ) );
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
        
        // If there is no provided position or the matching id can't be found so try it the hard way 
        try {
            DatabaseEntry keyEntry = new DatabaseEntry();
            DatabaseEntry valueEntry = new DatabaseEntry();
            valueEntry.setPartial(0, 0, true);
            
            AckIdV3 coreAckId = fullAckId.getCoreAckId();
            DatabaseEntry ackIdEntry = new DatabaseEntry();
            KEY_BINDING.objectToEntry(coreAckId, ackIdEntry);
            
            Cursor cursor = database.openCursor(txn_, CursorConfig.READ_UNCOMMITTED );
            try {
                cursor.setCacheMode(CacheMode.UNCHANGED);
                
                boolean reachedEnd = false;
                byte[] previousKeyData;
                int keyComparison = 0;
                long pos = 0;
                do {
                    // Note that getNextNoDup always uses a new array to return the result
                    previousKeyData = keyEntry.getData();
                    
                    OperationStatus result = cursor.getNextNoDup(keyEntry, valueEntry, null );
                    if( result == OperationStatus.NOTFOUND ) {
                        reachedEnd = true;
                        break;
                    }
                    
                    DatabaseUtil.checkSuccess(result, "getNextNoDup" );
                    pos++;
                    
                    keyComparison = DatabaseUtil.compareKeys( keyEntry, ackIdEntry );
                } while( keyComparison < 0 );
                
                if( reachedEnd ) {
                    // There is nothing >= to fulLAckId
                    AckIdV3 ackId;
                    if( previousKeyData == null ) {
                        assert pos == 0;
                        ackId = null;
                    } else {
                        ackId = new AckIdV3( new AckIdV3( previousKeyData ), pos );
                    }
                    return new StorePosition(bucketId, ackId);
                } else if( keyComparison == 0 ) {
                    // Got exact match with core of fullAckId
                    if( fullAckId.isInclusive() != null && !fullAckId.isInclusive() ) {
                        AckIdV3 ackId;
                        if( previousKeyData == null ) {
                            ackId = null;
                        } else {
                            ackId = new AckIdV3( new AckIdV3( previousKeyData ), pos - 1);
                        }
                        return new StorePosition(bucketId, ackId );
                    } else {
                        AckIdV3 ackId = new AckIdV3( new AckIdV3( keyEntry.getData() ), pos );
                        return new StorePosition(bucketId, ackId); 
                    }
                } else {
                    // There is no exact match so return the first one before
                    AckIdV3 ackId;
                    if( previousKeyData == null ) {
                        ackId = null;
                    } else {
                        ackId = new AckIdV3( new AckIdV3( previousKeyData ), pos - 1 );
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

    @Override
    public BucketCursor createCursor() {
        return new BDBBucketCursor(database, txn_);
    }
    
    @Override
    public void put(AckIdV3 key, StoredEntry<AckIdV3> value, boolean cache) 
        throws SeqStoreDatabaseException 
    {
        try {
            DatabaseEntry keyEntry = new DatabaseEntry();
            DatabaseEntry valueEntry = new DatabaseEntry();
            KEY_BINDING.objectToEntry(key, keyEntry);
            ENTRY_BINDING.objectToEntry(value, valueEntry);
            
            OperationStatus result;
            if( !cache ) {
                Transaction txn = database.getEnvironment().beginTransaction(
                        null, null);
                try {
                    Cursor cursor = database.openCursor(txn, null);
                    try {
                        cursor.setCacheMode(CacheMode.EVICT_LN);
                        cursor.put(keyEntry, valueEntry);
                    } finally {
                        cursor.close();
                    }
                    txn.commit();
                } finally {
                    txn.abort(); // Will abort the transaction if not already commited
                }
            } else {
                result = database.put(null, keyEntry, valueEntry);
                
                DatabaseUtil.checkSuccess(result, "put");
            }
        } catch (DatabaseException ex) {
            throw DatabaseUtil.translateBDBException( "Failed inserting into database", ex );
        }
    }
    
    @Override
    public void close() throws SeqStoreDatabaseException {
        try {
            // Shouldn't do anything as txn_ should only be used for ready operations.
            // use abort rather than commit as it can be called multiple times
            txn_.abort();
        } catch (DatabaseException e) {
            throw DatabaseUtil.translateBDBException("cant abort a transaction", e);
        } catch (IllegalArgumentException e ) {
            // This isn't a fatal error as database.closes the cursors for us and the transaction is fully cleaned
            // up even if this exception is thrown
            log.error( "There were open cursors when closing the bucket store", e );
        }

        try {
            database.close();
        } catch (DatabaseException e) {
            throw DatabaseUtil.translateBDBException( "Failed closing database", e );
        } catch (IllegalArgumentException e ) {
            // This isn't a fatal error as database.close closes the cursors for us before throwing the exception
            log.error( "There were open cursors when closing the bucket store", e );
        }
    }
    
    @Override
    public AckIdV3 getFirstId() throws SeqStoreDatabaseException {
        CursorConfig cursorConfig = new CursorConfig();
        cursorConfig.setReadUncommitted(true);
        try {
            Cursor cursor = database.openCursor(null, cursorConfig);
            try {
                DatabaseEntry keyEntry = new DatabaseEntry();
                DatabaseEntry valueEntry = new DatabaseEntry();
                valueEntry.setPartial(0, 0, true);
                cursor.setCacheMode(CacheMode.UNCHANGED);
                OperationStatus status = cursor.getFirst(keyEntry, valueEntry, null);
                if( status == OperationStatus.NOTFOUND ) {
                    return null;
                } 
                
                DatabaseUtil.checkSuccess(status, "getFirst");
                
                return KEY_BINDING.entryToObject(keyEntry);
            } finally {
                cursor.close();
            }
        } catch( DatabaseException e ) {
            throw DatabaseUtil.translateBDBException( "Failed getting first id", e );
        }
    }
    
    @Override
    public AckIdV3 getLastId() throws SeqStoreDatabaseException {
        CursorConfig cursorConfig = new CursorConfig();
        cursorConfig.setReadUncommitted(true);
        try {
            Cursor cursor = database.openCursor(null, cursorConfig);
            try {
                DatabaseEntry keyEntry = new DatabaseEntry();
                DatabaseEntry valueEntry = new DatabaseEntry();
                valueEntry.setPartial(0, 0, true);
                cursor.setCacheMode(CacheMode.UNCHANGED);
                OperationStatus status = cursor.getLast(keyEntry, valueEntry, null);
                if( status == OperationStatus.NOTFOUND ) {
                    return null;
                }
                
                DatabaseUtil.checkSuccess(status, "getLast");
                
                return KEY_BINDING.entryToObject(keyEntry);
            } finally {
                cursor.close();
            }
        } catch( DatabaseException e ) {
            throw DatabaseUtil.translateBDBException( "Failed getting last id", e );
        }
    }
    
    @Override
    public long getCount() throws SeqStoreDatabaseException {
        try {
            return database.count();
        } catch (DatabaseException e) {
            throw DatabaseUtil.translateBDBException( "Failed getting count.", e );
        }
    }
}
