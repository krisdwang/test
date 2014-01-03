package com.amazon.messaging.seqstore.v3.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;

public class AckIdV3Test {
    private void testByteConstructors(AckIdV3 ackId) {
        int expectedLength = ackId.getSerializedLength(); 
        byte ackIdBytes[] = ackId.toBytes();
        assertEquals( expectedLength, ackIdBytes.length );
        
        AckIdV3 fromBytes = new AckIdV3(ackIdBytes);
        assertEquals( fromBytes, ackId);
        assertEquals( fromBytes.getBucketPosition(), ackId.getBucketPosition());
        
        byte ackIdBytesInMiddle[] = new byte[ expectedLength + 5 ];
        ackId.writeToArray(ackIdBytesInMiddle, 3);
        assertArrayEquals( ackIdBytes, Arrays.copyOfRange(ackIdBytesInMiddle, 3, 3 + expectedLength ) );
        
        AckIdV3 fromByteRange = new AckIdV3( ackIdBytesInMiddle, 3, expectedLength );
        assertEquals( fromByteRange, ackId);
        assertEquals( fromByteRange.getBucketPosition(), ackId.getBucketPosition());
    }
    
    private void testConstructors(Boolean inclusive) {
        long sequence;
        int restartTime;
        
        if( inclusive == null || inclusive == false ) {
            sequence = 0;
            restartTime = 0;
        } else {
            sequence = Long.MAX_VALUE;
            restartTime = Integer.MAX_VALUE;
        }
        
        AckIdV3 a1 = new AckIdV3(2, sequence, restartTime, inclusive, 5L);
        testByteConstructors( a1 );
        
        AckIdV3 a2 = new AckIdV3(2, sequence, restartTime, inclusive, null);
        testByteConstructors( a2 );
        
        assertEquals(a1, a2);
        
        if( inclusive != null ) {
            AckIdV3 a3 = new AckIdV3(2, inclusive);
            testByteConstructors( a3 );
            
            AckIdV3 a4 = new AckIdV3(a3, inclusive);
            testByteConstructors( a4 );
            
            assertEquals(a3, a4);
            assertEquals(a3, a1);
            assertEquals(a3, a2);
            assertEquals(a4, a1);
            assertEquals(a4, a2);
        }
    }
    
    @Test
    public void testConstructors() {
        testConstructors( false );
        testConstructors( true );
        testConstructors( null );
    }

    @Test
    public void testComparison() {
        AckIdV3 a1 = new AckIdV3(0, false);
        AckIdV3 a2 = new AckIdV3(a1, true);
        assertTrue(a1.compareTo(a2) < 0);
        assertTrue(a2.compareTo(a1) > 0);
        assertTrue(AckIdV3.max(a1, a2) == a2);
        assertTrue(AckIdV3.min(a1, a2) == a1);
        AckIdV3 a3 = new AckIdV3(0, 0, 0);
        AckIdV3 a4 = new AckIdV3(a3, false);
        assertTrue(AckIdV3.min(a3, a4) == a4);
        assertTrue(AckIdV3.max(a3, a4) == a3);
        AckIdV3 a5 = new AckIdV3(a3, true);
        assertTrue(AckIdV3.min(a3, a5) == a3);
        assertTrue(AckIdV3.max(a3, a5) == a5);

        AckIdV3 a6 = new AckIdV3(0, 0, 0);
        AckIdV3 a7 = new AckIdV3(0, 0, 1);
        AckIdV3 a8 = new AckIdV3(0, 1, 0);
        AckIdV3 a9 = new AckIdV3(1, 0, 0);
        assertTrue(a6.compareTo(a6) == 0);
        assertTrue(a6.compareTo(a7) < 0);
        assertTrue(a6.compareTo(a8) < 0);
        assertTrue(a6.compareTo(a9) < 0);
        assertTrue(a7.compareTo(a6) > 0);
        assertTrue(a8.compareTo(a6) > 0);
        assertTrue(a9.compareTo(a6) > 0);
        assertTrue(a7.compareTo(a7) == 0);
        assertTrue(a7.compareTo(a8) > 0);
        assertTrue(a7.compareTo(a9) < 0);
        assertTrue(a8.compareTo(a7) < 0);
        assertTrue(a9.compareTo(a7) > 0);
        assertTrue(a8.compareTo(a8) == 0);
        assertTrue(a8.compareTo(a9) < 0);
        assertTrue(a9.compareTo(a8) > 0);

        assertEquals(a9, AckIdV3.max(a6, AckIdV3.max(a7, AckIdV3.max(a8, a9))));
        assertEquals(a6, AckIdV3.min(a6, AckIdV3.min(a7, AckIdV3.min(a8, a9))));
        
        AckIdV3 a10 = new AckIdV3(1, 0, 0, null, null );
        AckIdV3 a11 = new AckIdV3(1, 0, 0, null, 5L );
        assertEquals( a10, a11 );
        assertEquals( 0, a10.compareTo( a11 ) );
        assertEquals( 0, a11.compareTo( a10 ) );
    }

}
