package com.amazon.messaging.seqstore.v3.internal;

import org.junit.Test;
import static org.junit.Assert.*;

import com.amazon.messaging.impls.SettableClock;
import com.amazon.messaging.testing.TestCase;


public class DefaultAckIdSourceTest extends TestCase {
    @Test
    public void testEmpty() {
        AckIdGenerator ackIdGenerator = new AckIdGenerator();
        SettableClock clock = new SettableClock();
        DefaultAckIdSource source = new DefaultAckIdSource(ackIdGenerator, clock);
        AckIdV3 unusedAckId = ackIdGenerator.getAckId(1);
        
        assertNull( source.getMinimumUnfinishedEnqueue() );
        assertFalse( source.recordNewId( unusedAckId ) );
        assertFalse( source.isUnfinished( unusedAckId ) );
        assertEquals( clock.getCurrentTime(), source.getMinEnqueueLevel().getTime() );
        source.enqueueFinished( unusedAckId );
    }
    
    @Test
    public void testOneEnqueue() {
        AckIdGenerator ackIdGenerator = new AckIdGenerator();
        SettableClock clock = new SettableClock();
        DefaultAckIdSource source = new DefaultAckIdSource(ackIdGenerator, clock);
        AckIdV3 unusedAckId = ackIdGenerator.getAckId(1);
        
        AckIdV3 ackId = source.requestNewId(clock.getCurrentTime());
        
        assertEquals( ackId, source.getMinimumUnfinishedEnqueue() );
        assertTrue( source.isUnfinished( ackId ) );
        assertTrue( source.recordNewId( ackId ) );
        assertFalse( source.isUnfinished( unusedAckId ) );
        assertEquals( new AckIdV3( ackId, false ), source.getMinEnqueueLevel() );
        
        clock.increaseTime( 5 );
        assertEquals( new AckIdV3( ackId, false ), source.getMinEnqueueLevel() );
        
        source.enqueueFinished( ackId );
        
        assertNull( source.getMinimumUnfinishedEnqueue() );
        assertFalse( source.recordNewId( ackId ) );
        assertFalse( source.isUnfinished( ackId ) );
        assertTrue( source.getMinEnqueueLevel().compareTo( ackId ) > 0 );
        assertEquals( clock.getCurrentTime(), source.getMinEnqueueLevel().getTime() );
    }
    
    @Test
    public void testInOrderEnqueueCompletion() {
        AckIdGenerator ackIdGenerator = new AckIdGenerator();
        SettableClock clock = new SettableClock();
        DefaultAckIdSource source = new DefaultAckIdSource(ackIdGenerator, clock);
        
        AckIdV3 ackId1 = source.requestNewId(clock.getCurrentTime());
        AckIdV3 ackId2 = source.requestNewId(clock.getCurrentTime());
        
        assertEquals( ackId1, source.getMinimumUnfinishedEnqueue() );
        assertTrue( source.isUnfinished( ackId1 ) );
        assertTrue( source.isUnfinished( ackId2 ) );
        assertTrue( source.recordNewId( ackId1 ) );
        assertTrue( source.recordNewId( ackId2 ) );
        assertEquals( new AckIdV3( ackId1, false ), source.getMinEnqueueLevel() );
        
        source.enqueueFinished( ackId1 );
        
        assertFalse( source.isUnfinished( ackId1 ) );
        assertTrue( source.isUnfinished( ackId2 ) );
        assertFalse( source.recordNewId( ackId1 ) );
        assertTrue( source.recordNewId( ackId2 ) );
        assertEquals( new AckIdV3( ackId2, false ), source.getMinEnqueueLevel() );
        
        source.enqueueFinished( ackId2 );
        
        assertNull( source.getMinimumUnfinishedEnqueue() );
        assertFalse( source.recordNewId( ackId2 ) );
        assertFalse( source.isUnfinished( ackId2 ) );
        assertTrue( source.getMinEnqueueLevel().compareTo( ackId2 ) > 0 );
    }
    
    @Test
    public void testOutOfOrderEnqueueCompletion() {
        AckIdGenerator ackIdGenerator = new AckIdGenerator();
        SettableClock clock = new SettableClock();
        DefaultAckIdSource source = new DefaultAckIdSource(ackIdGenerator, clock);
        
        AckIdV3 ackId1 = source.requestNewId(clock.getCurrentTime());
        AckIdV3 ackId2 = source.requestNewId(clock.getCurrentTime());
        
        assertEquals( ackId1, source.getMinimumUnfinishedEnqueue() );
        assertTrue( source.isUnfinished( ackId1 ) );
        assertTrue( source.isUnfinished( ackId2 ) );
        assertTrue( source.recordNewId( ackId1 ) );
        assertTrue( source.recordNewId( ackId2 ) );
        assertEquals( new AckIdV3( ackId1, false ), source.getMinEnqueueLevel() );
        
        source.enqueueFinished( ackId2 );
        
        assertTrue( source.isUnfinished( ackId1 ) );
        assertFalse( source.isUnfinished( ackId2 ) );
        assertTrue( source.recordNewId( ackId1 ) );
        assertFalse( source.recordNewId( ackId2 ) );
        assertEquals( new AckIdV3( ackId1, false ), source.getMinEnqueueLevel() );
        
        source.enqueueFinished( ackId1 );
        
        assertNull( source.getMinimumUnfinishedEnqueue() );
        assertFalse( source.recordNewId( ackId1 ) );
        assertFalse( source.isUnfinished( ackId1 ) );
        assertTrue( source.getMinEnqueueLevel().compareTo( ackId2 ) > 0 );
    }
    
    @Test
    public void testRequestTimeAssignment() {
        AckIdGenerator ackIdGenerator = new AckIdGenerator();
        SettableClock clock = new SettableClock();
        DefaultAckIdSource source = new DefaultAckIdSource(ackIdGenerator, clock);
        
        clock.setCurrentTime( 5 );
        
        AckIdV3 ackId = source.requestNewId(1);
        assertEquals( 5, ackId.getTime() );
        source.enqueueFinished(ackId);
        
        ackId = source.requestNewId(5);
        assertEquals( 5, ackId.getTime() );
        source.enqueueFinished(ackId);
        
        ackId = source.requestNewId(7);
        assertEquals( 7, ackId.getTime() );
        source.enqueueFinished(ackId);
    }
}
