package com.amazon.messaging.seqstore.v3;

import java.io.File;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.amazon.messaging.seqstore.v3.config.SeqStoreImmutablePersistenceConfig;
import com.amazon.messaging.seqstore.v3.config.SeqStorePersistenceConfig;

public class SeqStorePersistenceConfigTest {
    @Test
    public void testEquals() {
        SeqStorePersistenceConfig config1 = new SeqStorePersistenceConfig();
        SeqStorePersistenceConfig config2 = new SeqStorePersistenceConfig();
        
        assertEquals( config1, config2 );
        
        config1.setDirtyMetadataFlushPeriod(5);
        assertEquals( 5, config1.getDirtyMetadataFlushPeriod() );
        assertFalse( config1.equals( config2 ) );
        config2.setDirtyMetadataFlushPeriod(5);
        assertEquals( config1, config2 );
        
        File jePropertiesFile = new File("/tmp/dne" );
        config1.setMainJePropertiesFile( jePropertiesFile );
        assertEquals( jePropertiesFile, config1.getMainJePropertiesFile() );
        assertFalse( config1.equals( config2 ) );
        config2.setMainJePropertiesFile( jePropertiesFile );
        assertEquals( config1, config2 );
        
        config1.setOpenReadOnly(true);
        assertEquals( true, config1.isOpenReadOnly() );
        assertFalse( config1.equals( config2 ) );
        config2.setOpenReadOnly(true);
        assertEquals( config1, config2 );
        
        File storeDirectory = new File("/tmp/dne2");
        config1.setStoreDirectory( storeDirectory);
        assertEquals( storeDirectory, config1.getStoreDirectory() );
        assertFalse( config1.equals( config2 ) );
        config2.setStoreDirectory( storeDirectory);
        assertEquals( config1, config2 );
        
        config1.setTruncateDatabase(true);
        assertEquals( true, config1.isTruncateDatabase() );
        assertFalse( config1.equals( config2 ) );
        config2.setTruncateDatabase(true);
        assertEquals( config1, config2 );
    }
    
    @Test
    public void testCopy() {
        SeqStorePersistenceConfig config1 = new SeqStorePersistenceConfig();
        
        File jePropertiesFile = new File("/tmp/dne" );
        File storeDirectory = new File("/tmp/dne2");
        config1.setDirtyMetadataFlushPeriod(5);
        config1.setMainJePropertiesFile( jePropertiesFile );
        config1.setOpenReadOnly(true);
        config1.setStoreDirectory( storeDirectory);
        config1.setTruncateDatabase(true);
        
        SeqStorePersistenceConfig config2 = new SeqStorePersistenceConfig(config1);
        assertEquals( config1, config2 );
    }
    
    @Test
    public void testGetImmutableConfig() {
        SeqStorePersistenceConfig config1 = new SeqStorePersistenceConfig();
        
        File propertiesFile = new File("/tmp/dne" );
        File storeFile = new File("/tmp/dne2" );
        config1.setDirtyMetadataFlushPeriod(5);
        config1.setMainJePropertiesFile( propertiesFile );
        config1.setOpenReadOnly(true);
        config1.setStoreDirectory( storeFile );
        config1.setTruncateDatabase(false);
        
        SeqStoreImmutablePersistenceConfig config2 = config1.getImmutableConfig();
        assertEquals( 5, config2.getDirtyMetadataFlushPeriod() );
        assertEquals( propertiesFile, config2.getMainJePropertiesFile() ); 
        assertEquals( storeFile, config2.getStoreDirectory() );
        assertEquals( true, config2.isOpenReadOnly() );
        assertEquals( false, config2.isTruncateDatabase() );
    }
}
