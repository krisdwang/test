package com.amazon.messaging.seqstore.v3.bdb;

import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreDatabaseException;
import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreUnrecoverableDatabaseException;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Sequence;
import com.sleepycat.je.SequenceConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.tree.Key;

public final class DatabaseUtil {

    public static final Database createDB(Environment env, String name, boolean allowCreate,
                                          boolean allowDuplicate, boolean readOnly) throws DatabaseException
    {
        Transaction txn;
        TransactionConfig tconfig = new TransactionConfig();
        tconfig.setReadCommitted(true);
        txn = env.beginTransaction(null, tconfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(allowCreate && !readOnly);
        dbConfig.setTransactional(true);
        dbConfig.setReadOnly(readOnly);
        if (allowDuplicate) {
            dbConfig.setSortedDuplicates(true);
        }
        Database db;
        try {
            db = env.openDatabase(txn, name, dbConfig);
            txn.commit();
        } catch (DatabaseException e) {
            try {
                txn.abort();
            } catch (DatabaseException e1) {
                // Ignoring we are throwing anyway.
            }
            throw e;
        }
        return db;
    }

    public static final Database createDB(
        Environment env, Transaction txn, String name, boolean allowCreate, boolean readOnly)
            throws DatabaseException 
    {
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setReadOnly(readOnly);
        dbConfig.setAllowCreate(allowCreate && !readOnly);
        dbConfig.setSortedDuplicates(false);
        return getdb(env, txn, name, dbConfig);
    }

    /**
     * Separate Function for the sake of profiling data.
     */
    private static Database getdb(Environment env, Transaction txn, String name, DatabaseConfig dbConfig)
            throws DatabaseException {
        return getDbHelper(env, txn, name, dbConfig);
    }

    /**
     * Separate Function for the sake of profiling data.
     */
    private static Database getDbHelper(Environment env, Transaction txn, String name, DatabaseConfig dbConfig)
            throws DatabaseException {
        return env.openDatabase(txn, name, dbConfig);
    }
    
    /**
     * Translate a DatabaseException into a SeqStoreDatabaseException exception.
     * 
     * @param message the message to put into the SeqStoreDatabaseException
     * @param e the DatabaseException
     * @return
     */
    public static SeqStoreDatabaseException translateBDBException(String message, DatabaseException e) {
        if( e instanceof EnvironmentFailureException ) {
            return new SeqStoreUnrecoverableDatabaseException( 
                    "BDB Corruption Detected", "BDB Corruption Detected: " + message, e );
        } else {
            return new SeqStoreDatabaseException( message, e );
        }
    }
    
    /**
     * Translate a DatabaseException into a SeqStoreDatabaseException exception.
     * 
     * @param message the message to put into the SeqStoreDatabaseException
     * @param e the DatabaseException
     * @return
     */
    public static SeqStoreDatabaseException translateBDBException(DatabaseException e) {
        return translateBDBException( e.getMessage(), e );
    }
    
    public static SeqStoreDatabaseException translateWrappedBDBException(String message, Exception e) {
        Throwable nested = e;
        while( nested != null && !(nested instanceof DatabaseException) ) {
            nested = nested.getCause();
        }
        
        if( nested == null ) {
            throw new IllegalArgumentException( e + " was not caused by a DatabaseException", e );
        }
        
        if( nested instanceof EnvironmentFailureException ) {
            return new SeqStoreUnrecoverableDatabaseException( 
                    "BDB Corruption Detected", "BDB Corruption Detected", e );
        } else {
            return new SeqStoreDatabaseException( message, e );
        }
    }

    public static SeqStoreDatabaseException translateWrappedBDBException(Exception e) {
        return translateWrappedBDBException(e.getMessage(), e);
    }
    
    public static Sequence createSequence(
        Database database, String sequenceName, boolean allowCreate, int cacheSize)
    {
        SequenceConfig sequenceConfig = new SequenceConfig();
        sequenceConfig.setAllowCreate(allowCreate);
        sequenceConfig.setCacheSize(cacheSize);
        
        TupleOutput tupleOutput = new TupleOutput();
        tupleOutput.writeString(sequenceName);
        
        DatabaseEntry sequenceEntry = new DatabaseEntry();
        sequenceEntry.setData( tupleOutput.toByteArray() );
        
        return database.openSequence(
                null, sequenceEntry, sequenceConfig);
    }

    /**
     * Check the result is OperationStatus.SUCCESS  and thrown a SeqStoreDatabaseException if
     * it is not.
     * 
     * @param result the result to check
     * @param op the operation to use in the exception text on failure
     * @throws SeqStoreDatabaseException
     */
    static void checkSuccess( OperationStatus result, String op ) throws SeqStoreDatabaseException {
        if( result != OperationStatus.SUCCESS ) {
            throw new SeqStoreDatabaseException( "Unexpected result " + result + " from " + op );
        }
    }
    
    /**
     * Compare two database entries as keys the same way BDB compares them. This function
     * should have the same results as calling
     * <code>{@link Key#compareKeys}( {@link Key#makeKey}( key1 ), {@link Key#makeKey}( key2 ), null )</code>
     * but without the array copies done by {@link Key#makeKey}.
     * 
     * @return -1, 0, or 1 if key1 is less than, equal to, or greater than key2 as sorted by BDB
     */
    public static int compareKeys(DatabaseEntry key1, DatabaseEntry key2) {
        byte[] key1Bytes = key1.getData();
        byte[] key2Bytes = key2.getData();
        
        if( key1Bytes == key2Bytes ) {
            return 0;
        } else if( key1Bytes == null ) {
            if( key2Bytes.length == 0 ) return 0;
            return 1;
        } else if( key2Bytes == null ) {
            if( key1Bytes.length == 0 ) return 0;
            return -1;
        }
        
        int key1Len = key1.getSize();
        int key2Len = key2.getSize();

        int limit = Math.min(key1Len, key2Len);

        for (int i = 0; i < limit; i++) {
            byte b1 = key1Bytes[i];
            byte b2 = key2Bytes[i];
            if (b1 != b2) {
                /** 
                 * Compare the bytes as unsigned as that is how BDB sorts them  
                 */
                return (b1 & 0xff) - (b2 & 0xff);
            }
        }

        return (key1Len - key2Len);
    }
}
