package com.amazon.messaging.seqstore.v3.internal;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.Test;
import static org.junit.Assert.*;

import com.amazon.messaging.seqstore.v3.Checkpoint;
import com.amazon.messaging.seqstore.v3.CheckpointEntryInfo;
import com.amazon.messaging.seqstore.v3.InflightMetrics;
import com.amazon.messaging.seqstore.v3.InflightUpdateRequest;
import com.amazon.messaging.seqstore.v3.InflightUpdateResult;
import com.amazon.messaging.seqstore.v3.internal.ReaderInflightData.RedeliverySchedule;
import com.amazon.messaging.testing.TestCase;
import com.amazon.messaging.util.BasicInflightInfo;
import com.amazon.messaging.utils.Pair;

public class ReaderInflightDataTest extends TestCase {

    @Test
    public void testEmpty() {
        AckIdV3 minAckId = new AckIdV3( 0, false );
        AckIdV3 testAck = new AckIdV3( 2, 0, 1, null, 5L );
        ReaderInflightData<BasicInflightInfo> readerInflightData = new ReaderInflightData<BasicInflightInfo>();
        assertTrue( readerInflightData.isEmpty() );
        assertNull( readerInflightData.peekFirstScheduled( Long.MAX_VALUE ) );
        assertTrue( readerInflightData.getAllEntries().isEmpty() );
        assertNull( readerInflightData.getInflightEntry( testAck ) );
        assertEquals( InflightMetrics.ZeroInFlight, readerInflightData.getMetrics(Long.MAX_VALUE) );
        assertFalse( readerInflightData.isInflight( testAck ) );
        assertEquals( Long.MAX_VALUE, readerInflightData.getTimeOfNextTimeout() );
        assertEquals( minAckId, readerInflightData.getAckLevel() );
        assertEquals( Collections.emptyList(), readerInflightData.getAllEntries() );
        readerInflightData.makeAllMessagseDeliverable(Long.MAX_VALUE);
        
        Checkpoint<AckIdV3, AckIdV3, BasicInflightInfo> checkpoint = readerInflightData.getCheckpoint(-1);
        assertTrue( checkpoint.getInflightMessages().isEmpty() );
        assertEquals( minAckId, checkpoint.getReadLevel() );
        assertEquals( minAckId, checkpoint.getAckLevel() );
        
        checkpoint = readerInflightData.getCheckpoint( 1 );
        assertTrue( checkpoint.getInflightMessages().isEmpty() );
        assertEquals( minAckId, checkpoint.getReadLevel() );
        assertEquals( minAckId, checkpoint.getAckLevel() );

        readerInflightData.ack( testAck );
        assertEquals( minAckId, readerInflightData.getAckLevel() );
        readerInflightData.setAckLevel( testAck );
        assertEquals( new AckIdV3( testAck, true), readerInflightData.getAckLevel() );
    }

    @Test
    public void testDoubleAdd() {
        AckIdV3 ack1 = new AckIdV3( 1, 0, 1 );
        ReaderInflightData<BasicInflightInfo> readerInflightData = new ReaderInflightData<BasicInflightInfo>();

        BasicInflightInfo info1 = new BasicInflightInfo(1);
        readerInflightData.add(ack1, 5, info1);

        BasicInflightInfo info2 = new BasicInflightInfo(1);
        try {
            readerInflightData.add(ack1, 6, info2);
            fail( "Double add did not throw.");
        } catch( IllegalStateException e ) {
            // We want to get here
        }
    }
    
    private static InflightUpdateResult reschedule( ReaderInflightData<BasicInflightInfo> readerInflightData, AckIdV3 ackId, long newTime ) {
        return readerInflightData.update(ackId, 0,
                new InflightUpdateRequest<BasicInflightInfo>()
                        .withNewTimeout(newTime) );
    }
    
    @Test
    public void testExpect() {
        AckIdV3 ackid = new AckIdV3( 1, 0, 1, null, 1L );
        
        ReaderInflightData<BasicInflightInfo> readerInflightData = new ReaderInflightData<BasicInflightInfo>();
        
        BasicInflightInfo info1 = new BasicInflightInfo(1);
        BasicInflightInfo info2 = new BasicInflightInfo(2);
        BasicInflightInfo info3 = new BasicInflightInfo(3);
        
        assertEquals( 
                InflightUpdateResult.NOT_FOUND,
                readerInflightData.update(
                        ackid, 0,
                        new InflightUpdateRequest<BasicInflightInfo>()
                            .withNewTimeout(1) ) );
        
        assertEquals( 
                InflightUpdateResult.NOT_FOUND,
                readerInflightData.update(
                        ackid, 0,
                        new InflightUpdateRequest<BasicInflightInfo>()
                            .withExpectedInflightInfo(info1)
                            .withNewInflightInfo(info2)
                            .withNewTimeout(1) ) );
        
        readerInflightData.add( ackid, 1, null );
        
        Pair<RedeliverySchedule, BasicInflightInfo> inflightEntry = readerInflightData.getInflightEntry(ackid);
        assertNotNull( inflightEntry );
        assertEquals( null, inflightEntry.getValue() );
        
        assertEquals( 
                InflightUpdateResult.EXPECTATION_UNMET,
                readerInflightData.update(
                        ackid, 0,
                        new InflightUpdateRequest<BasicInflightInfo>()
                            .withExpectedInflightInfo(info1)
                            .withNewInflightInfo(info2)
                            .withNewTimeout(1) ) );
        
        inflightEntry = readerInflightData.getInflightEntry(ackid);
        assertNotNull( inflightEntry );
        assertEquals( null, inflightEntry.getValue() );
        
        assertEquals( 
                InflightUpdateResult.DONE,
                readerInflightData.update(
                        ackid, 0,
                        new InflightUpdateRequest<BasicInflightInfo>()
                            .withExpectedInflightInfo(null)
                            .withNewInflightInfo(info1)
                            .withNewTimeout(1) ) );
        
        inflightEntry = readerInflightData.getInflightEntry(ackid);
        assertNotNull( inflightEntry );
        assertEquals( info1, inflightEntry.getValue() );
        
        assertEquals( 
                InflightUpdateResult.EXPECTATION_UNMET,
                readerInflightData.update(
                        ackid, 0,
                        new InflightUpdateRequest<BasicInflightInfo>()
                            .withExpectedInflightInfo(info2)
                            .withNewInflightInfo(info3)
                            .withNewTimeout(1) ) );
        
        inflightEntry = readerInflightData.getInflightEntry(ackid);
        assertNotNull( inflightEntry );
        assertEquals( info1, inflightEntry.getValue() );
        
        assertEquals( 
                InflightUpdateResult.EXPECTATION_UNMET,
                readerInflightData.update(
                        ackid, 0,
                        new InflightUpdateRequest<BasicInflightInfo>()
                            .withExpectedInflightInfo(null)
                            .withNewInflightInfo(info2)
                            .withNewTimeout(1) ) );
        
        inflightEntry = readerInflightData.getInflightEntry(ackid);
        assertNotNull( inflightEntry );
        assertEquals( info1, inflightEntry.getValue() );
        
        assertEquals( 
                InflightUpdateResult.DONE,
                readerInflightData.update(
                        ackid, 0,
                        new InflightUpdateRequest<BasicInflightInfo>()
                            .withExpectedInflightInfo(new BasicInflightInfo(1))
                            .withNewInflightInfo(info2) ) );
        
        inflightEntry = readerInflightData.getInflightEntry(ackid);
        assertNotNull( inflightEntry );
        assertEquals( info2, inflightEntry.getValue() );
    }
    
    @Test
    public void testTakeFirstScheduled() {
        AckIdV3 ackid = new AckIdV3( 1, 0, 1, null, 1L );
        
        ReaderInflightData<BasicInflightInfo> readerInflightData = new ReaderInflightData<BasicInflightInfo>();
        
        BasicInflightInfo info1 = new BasicInflightInfo(1);
        BasicInflightInfo info2 = new BasicInflightInfo(2);
        
        assertEquals( null, readerInflightData.takeFirstScheduled(5) );
        
        readerInflightData.add( ackid, 6, info1 );
        
        assertEquals( null, readerInflightData.takeFirstScheduled(5) );
        
        assertEquals( 
                new Pair<RedeliverySchedule, BasicInflightInfo>( new RedeliverySchedule(ackid, 6), info1 ),
                readerInflightData.takeFirstScheduled(6) );
        
        assertEquals( null, readerInflightData.takeFirstScheduled(6) );
        
        assertEquals( 
                InflightUpdateResult.DONE,
                readerInflightData.update(
                        ackid, 6,
                        new InflightUpdateRequest<BasicInflightInfo>()
                            .withExpectedInflightInfo(new BasicInflightInfo(1))
                            .withNewInflightInfo(info2)
                            .withNewTimeout(2) ) );
        
        assertEquals( null, readerInflightData.takeFirstScheduled(6) );
        
        assertEquals( 
                new Pair<RedeliverySchedule, BasicInflightInfo>( new RedeliverySchedule(ackid, 8), info2 ),
                readerInflightData.takeFirstScheduled(10) );
    }

    @Test
    public void testReschedule() {
        AckIdV3 notInflightAckId = new AckIdV3( 1, 3, 1, null, 0L );
        AckIdV3 ack1 = new AckIdV3( 1, 0, 1, null, 1L );
        AckIdV3 ack2 = new AckIdV3( 1, 1, 1, null, 2L );
        AckIdV3 ack3 = new AckIdV3( 1, 2, 1, null, 3L );
        final int currentTime = 30;

        // Message :   1  | 2  | 3
        // Timeout :   6  | 6  | 9

        long notInflightTime = 5;

        BasicInflightInfo info1 = new BasicInflightInfo(1);
        BasicInflightInfo info2 = new BasicInflightInfo(1);
        BasicInflightInfo info3 = new BasicInflightInfo(1);

        long time1 = 6;
        long time2 = 6;
        long time3 = 9;

        ReaderInflightData<BasicInflightInfo> readerInflightData = new ReaderInflightData<BasicInflightInfo>();

        assertEquals( 
                InflightUpdateResult.NOT_FOUND,
                reschedule( readerInflightData, notInflightAckId, notInflightTime ) );
                        
        assertEquals( Long.MAX_VALUE, readerInflightData.getTimeOfNextTimeout() );
        assertNull( readerInflightData.peekFirstScheduled( currentTime ) );

        readerInflightData.add( ack1, time1, info1 );
        readerInflightData.add( ack2, time2, info2 );
        readerInflightData.add( ack3, time3, info3 );

        assertEquals( 6, readerInflightData.getTimeOfNextTimeout() );

        // Nothing should be available at time 1
        assertNull( readerInflightData.peekFirstScheduled( 1 ) );

        // Take the first message and reschedule it after the second message but before the third
        Pair<RedeliverySchedule, BasicInflightInfo> peek1 = readerInflightData.peekFirstScheduled( currentTime );
        assertNotNull(peek1);
        assertEquals(ack1, peek1.getKey().getAckId());
        assertEquals(time1, peek1.getKey().getRedeliveryTime());
        assertEquals(info1, peek1.getValue());
        long rescheduleTime1 = 8;
        assertEquals( InflightUpdateResult.DONE, reschedule( readerInflightData, ack1, rescheduleTime1 ) );

        // Message :   2  | 1 | 3
        // Timeout :   6  | 8 | 9
        assertEquals( 6, readerInflightData.getTimeOfNextTimeout() );

        // Take the second message and reschedule it after the third message
        Pair<RedeliverySchedule, BasicInflightInfo> peek2 = readerInflightData.peekFirstScheduled( currentTime );
        assertNotNull(peek2);
        assertEquals(ack2, peek2.getKey().getAckId());
        assertEquals(time2, peek2.getKey().getRedeliveryTime());
        assertEquals(info2, peek2.getValue());
        long rescheduleTime2 = 12;
        assertEquals( InflightUpdateResult.DONE, reschedule( readerInflightData, ack2, rescheduleTime2 ) );

        // Message :   1  |  2 | 3
        // Timeout :   8  | 12 | 9

        assertEquals( 8, readerInflightData.getTimeOfNextTimeout() );

        // Take the first message from its new time and reschedule it between the 2nd and 3rd messages
        Pair<RedeliverySchedule, BasicInflightInfo> peek3 = readerInflightData.peekFirstScheduled( currentTime );
        assertNotNull(peek3);
        assertEquals(ack1, peek3.getKey().getAckId());
        assertEquals(rescheduleTime1, peek3.getKey().getRedeliveryTime());
        assertEquals(info1, peek3.getValue());

        long rescheduleTime3 = 11;
        assertEquals( InflightUpdateResult.DONE, reschedule( readerInflightData, ack1, rescheduleTime3 ) );

        // Message :   1  |  2 | 3
        // Timeout :  11  | 12 | 9

        assertEquals( 9, readerInflightData.getTimeOfNextTimeout() );

        Pair<RedeliverySchedule, BasicInflightInfo> peek4 = readerInflightData.peekFirstScheduled( currentTime );
        assertNotNull(peek4);
        assertEquals(ack3, peek4.getKey().getAckId());
        assertEquals(time3, peek4.getKey().getRedeliveryTime());
        assertEquals(info3, peek4.getValue());

        // Get the third message by id
        Pair<RedeliverySchedule, BasicInflightInfo> entry3info = readerInflightData.getInflightEntry(ack3);
        assertNotNull(entry3info);
        assertEquals(ack3, entry3info.getKey().getAckId());
        assertEquals(time3, entry3info.getKey().getRedeliveryTime());
        assertEquals(info3, entry3info.getValue());

        // Reschedule it before the other messages
        long rescheduleTime4 = 10;
        assertEquals( InflightUpdateResult.DONE, reschedule( readerInflightData, ack3, rescheduleTime4 ) );

        // Message :   1  |  2 |  3
        // Timeout :  11  | 12 | 10

        assertEquals( 10, readerInflightData.getTimeOfNextTimeout() );

        // Get the third message again
        Pair<RedeliverySchedule, BasicInflightInfo> peek5 = readerInflightData.peekFirstScheduled( currentTime );
        assertNotNull(peek5);
        assertEquals(ack3, peek5.getKey().getAckId());
        assertEquals(rescheduleTime4, peek5.getKey().getRedeliveryTime());
        assertEquals(info3, peek5.getValue());

        // Test a message not inflight being updated fails
        assertEquals( 
                InflightUpdateResult.NOT_FOUND,
                reschedule( readerInflightData, notInflightAckId, notInflightTime ) );
        assertNull( readerInflightData.getInflightEntry( notInflightAckId ) );
    }
    
    @Test
    public void testAck() {
        AckIdV3 notInflightAckId = new AckIdV3( 1, 3, 1, null, 0L );
        AckIdV3 ack1 = new AckIdV3( 1, 0, 1, null, 1L );
        AckIdV3 ack2 = new AckIdV3( 1, 1, 1, null, 2L );
        AckIdV3 ack3 = new AckIdV3( 1, 2, 1, null, 3L );
        int currentTime = 30;

        BasicInflightInfo info1 = new BasicInflightInfo(1);
        BasicInflightInfo info2 = new BasicInflightInfo(1);
        BasicInflightInfo info3 = new BasicInflightInfo(1);

        long time1 = 6;
        long time2 = 6;
        long time3 = 9;

        ReaderInflightData<BasicInflightInfo> readerInflightData = new ReaderInflightData<BasicInflightInfo>();

        assertFalse( readerInflightData.isInflight( notInflightAckId ) ) ;
        assertEquals( Long.MAX_VALUE, readerInflightData.getTimeOfNextTimeout() );
        assertEquals( new AckIdV3(0, false ), readerInflightData.getAckLevel() );

        readerInflightData.add( ack1, time1, info1 );
        readerInflightData.add( ack2, time2, info2 );
        readerInflightData.add( ack3, time3, info3 );

        // Message :   1  |  2 |  3
        // Timeout :   6  |  6 |  8

        assertEquals( new AckIdV3( ack1, false ), readerInflightData.getAckLevel() );
        assertEquals( new InflightMetrics(3, 0), readerInflightData.getMetrics(5) );
        assertEquals( new InflightMetrics(1, 2), readerInflightData.getMetrics(7) );
        assertEquals( new InflightMetrics(0, 3), readerInflightData.getMetrics(9) );

        assertTrue( readerInflightData.isInflight( ack1 ) );
        assertTrue( readerInflightData.isInflight( ack2 ) );
        assertTrue( readerInflightData.isInflight( ack3 ) );
        assertEquals( 6, readerInflightData.getTimeOfNextTimeout() );

        // Message :   1  |  2 |  3
        // Timeout :   6  |  6 |  A

        // Ack the last message
        readerInflightData.ack( ack3 );

        assertEquals( 2, readerInflightData.getMetrics(currentTime).getNumAvailableForRedelivery());
        assertEquals( new AckIdV3( ack1, false ), readerInflightData.getAckLevel() );
        assertTrue( readerInflightData.isInflight( ack1 ) );
        assertTrue( readerInflightData.isInflight( ack2 ) );
        assertFalse( readerInflightData.isInflight( ack3 ) );
        assertEquals( 6, readerInflightData.getTimeOfNextTimeout() );

        // Message :   1  |  2 |  3
        // Timeout :   A  |  6 |  A

        // Ack the first message
        readerInflightData.ack( ack1 );

        assertEquals( 1, readerInflightData.getMetrics(currentTime).getNumAvailableForRedelivery());
        assertEquals( new AckIdV3( ack2, false ), readerInflightData.getAckLevel() );
        assertEquals( 6, readerInflightData.getTimeOfNextTimeout() );

        Pair<RedeliverySchedule, BasicInflightInfo> entry2info = readerInflightData.getInflightEntry(ack2);
        assertNotNull(entry2info);
        assertEquals(ack2, entry2info.getKey().getAckId());
        assertEquals(time2, entry2info.getKey().getRedeliveryTime());
        assertEquals(info2, entry2info.getValue());

        // Reschedule it
        long rescheduleTime1 = 11;
        assertEquals( InflightUpdateResult.DONE, reschedule(readerInflightData, ack2, rescheduleTime1) );

        // Message :   1  |  2 |  3
        // Timeout :   A  | 11 |  A
        assertEquals( 11, readerInflightData.getTimeOfNextTimeout() );

        // Ack message 2
        readerInflightData.ack( ack2 );

        // Message :   1  |  2 |  3
        // Timeout :   A  |  A |  A
        assertEquals( new AckIdV3( ack3, true ), readerInflightData.getAckLevel() );
        assertTrue( readerInflightData.isEmpty() );
        assertEquals( InflightMetrics.ZeroInFlight, readerInflightData.getMetrics( currentTime ) );

        // Updating the message should do nothing
        long rescheduleTime2 = 15;
        assertEquals( InflightUpdateResult.NOT_FOUND, reschedule(readerInflightData, ack2, rescheduleTime2) );
        assertEquals( Long.MAX_VALUE, readerInflightData.getTimeOfNextTimeout() );
        assertTrue( readerInflightData.isEmpty() );
        assertEquals( InflightMetrics.ZeroInFlight, readerInflightData.getMetrics( Long.MAX_VALUE ) );
    }

    @Test
    public void testSetAckLevel() {
        AckIdV3 ack1 = new AckIdV3( 1, 0, 1, null, 1L );
        AckIdV3 ack2 = new AckIdV3( 1, 1, 1, null, 2L );
        AckIdV3 middleAck = new AckIdV3( 1, 2, 1, null, 3L );
        AckIdV3 ack3 = new AckIdV3( 1, 3, 1, null, 4L );

        BasicInflightInfo info1 = new BasicInflightInfo(1);
        BasicInflightInfo info2 = new BasicInflightInfo(1);
        BasicInflightInfo info3 = new BasicInflightInfo(1);

        long time1 = 6;
        long time2 = 6;
        long time3 = 9;

        ReaderInflightData<BasicInflightInfo> readerInflightData = new ReaderInflightData<BasicInflightInfo>();

        readerInflightData.add( ack1, time1, info1 );
        readerInflightData.add( ack2, time2, info2 );
        readerInflightData.add( ack3, time3, info3 );

        // Message :   1  |  2 |  3
        // Timeout :   6  |  6 |  9
        assertEquals( new AckIdV3( ack1, false ), readerInflightData.getAckLevel() );
        assertEquals( 1, readerInflightData.getMetrics(6).getNumInFlight());
        assertEquals( 2, readerInflightData.getMetrics(6).getNumAvailableForRedelivery());
        assertEquals( 3, readerInflightData.getMetrics(9).getNumAvailableForRedelivery());
        assertEquals( 3, readerInflightData.getMetrics(Long.MAX_VALUE).getNumAvailableForRedelivery());

        readerInflightData.setAckLevel( middleAck );

        // Message :   1  |  2 |  3
        // Timeout :   A  |  A |  9
        assertEquals( new AckIdV3( ack3, false ), readerInflightData.getAckLevel() );
        assertEquals( new InflightMetrics(0,1), readerInflightData.getMetrics(Long.MAX_VALUE) );
        assertEquals( 9, readerInflightData.getTimeOfNextTimeout() );

        Pair<RedeliverySchedule, BasicInflightInfo> peek1 = readerInflightData.peekFirstScheduled(Long.MAX_VALUE);
        assertNotNull(peek1);
        assertEquals(ack3, peek1.getKey().getAckId());
        assertEquals(time3, peek1.getKey().getRedeliveryTime());
        assertEquals(info3, peek1.getValue());

        readerInflightData.setAckLevel( ack3 );

        // Message :   1  |  2 |  3
        // Timeout :   A  |  A |  A
        assertTrue( readerInflightData.isEmpty() );
        assertEquals( new AckIdV3( ack3, true ), readerInflightData.getAckLevel() );
        assertEquals( InflightMetrics.ZeroInFlight, readerInflightData.getMetrics(Long.MAX_VALUE) );
    }
    
    @Test
    public void testMakeAllMessagesDeliverable() {
        AckIdV3 ack1 = new AckIdV3( 1, 0, 1, null, 1L );
        AckIdV3 ack2 = new AckIdV3( 1, 1, 1, null, 2L );
        AckIdV3 ack3 = new AckIdV3( 1, 2, 1, null, 3L );

        BasicInflightInfo info1 = new BasicInflightInfo(1);
        BasicInflightInfo info2 = new BasicInflightInfo(1);
        BasicInflightInfo info3 = new BasicInflightInfo(1);

        long time1 = 5;
        long time2 = 7;
        long time3 = 9;

        ReaderInflightData<BasicInflightInfo> readerInflightData = new ReaderInflightData<BasicInflightInfo>();

        readerInflightData.add( ack1, time1, info1 );
        readerInflightData.add( ack2, time2, info2 );
        readerInflightData.add( ack3, time3, info3 );

        // Message :   1  |  2 |  3
        // Timeout :   5  |  7 |  9

        assertEquals( new AckIdV3( ack1, false ), readerInflightData.getAckLevel() );
        assertEquals( new InflightMetrics(2, 1), readerInflightData.getMetrics(6) );
        assertEquals( 5, readerInflightData.getTimeOfNextTimeout() );

        readerInflightData.makeAllMessagseDeliverable( 6 );

        // Message :   1  |  2 |  3
        // Timeout :   5  |  6 |  6
        assertEquals( new InflightMetrics(0, 3), readerInflightData.getMetrics(6) );
        assertEquals( 5, readerInflightData.getTimeOfNextTimeout() );

        long rescheduleTime1 = 10;
        reschedule(readerInflightData, ack1, rescheduleTime1);

        // Message:    2 |   3 |  1
        // Timeout:    6 |   6 | 10
        assertEquals( new InflightMetrics(1, 2), readerInflightData.getMetrics(6) );
        assertEquals( 6, readerInflightData.getTimeOfNextTimeout() );
    }

    @Test
    public void testEmptyInflightGetSetAckLevel() {
        AckIdV3 nullAck = new AckIdV3( 0, false );
        AckIdV3 ack1 = new AckIdV3( 1, 1, 1, false, 5L );
        AckIdV3 ack2 = new AckIdV3( 1, 1, 1, true, 7L );

        ReaderInflightData<BasicInflightInfo> readerInflightData = new ReaderInflightData<BasicInflightInfo>();
        assertEquals( nullAck, readerInflightData.getAckLevel() );

        readerInflightData.setAckLevel( ack1 );
        assertEquals( ack1, readerInflightData.getAckLevel() );

        readerInflightData.setAckLevel( ack2 );
        assertEquals( ack2, readerInflightData.getAckLevel() );
    }
    
    @Test
    public void testFullCheckpointContents() {
        final int NUM_MESSAGES = 200;
        final AckIdV3 minAckId = new AckIdV3(0, false);
        final BasicInflightInfo info1 = new BasicInflightInfo(1);
        final BasicInflightInfo info2 = new BasicInflightInfo(2);
        long time = 5;
        
        Checkpoint<AckIdV3, AckIdV3, BasicInflightInfo> checkpoint;
        AckIdGenerator idGenerator = new AckIdGenerator(time);
        ReaderInflightData<BasicInflightInfo> readerInflightData = new ReaderInflightData<BasicInflightInfo>();
        AckIdV3 readLevel = minAckId; 
        
        checkpoint = readerInflightData.getCheckpoint(-1);
        assertTrue( checkpoint.getInflightMessages().isEmpty() );
        assertEquals( minAckId, checkpoint.getAckLevel() );
        assertEquals( readLevel, checkpoint.getReadLevel() );
        
        NavigableMap<AckIdV3, Pair<BasicInflightInfo, RedeliverySchedule>> messageMap = 
            new TreeMap<AckIdV3, Pair<BasicInflightInfo,RedeliverySchedule>>();
        
        addMessagesToInflight(messageMap, time, 10, NUM_MESSAGES / 2, idGenerator, info1, readerInflightData );
        addMessagesToInflight(messageMap, time + 2, 15, NUM_MESSAGES / 2, idGenerator, info2, readerInflightData );
        readLevel = new AckIdV3( messageMap.lastKey(), true );
        
        checkpoint = readerInflightData.getCheckpoint(-1);
        assertEquals( readerInflightData.getAckLevel(), checkpoint.getAckLevel() );
        assertEquals( readLevel, checkpoint.getReadLevel() );
        assertEquals( messageMap.size(), checkpoint.getInflightMessages().size() );
        
        for( CheckpointEntryInfo<AckIdV3, BasicInflightInfo> entry : checkpoint.getInflightMessages() ) {
            Pair<BasicInflightInfo, RedeliverySchedule> messageInfo = messageMap.get( entry.getAckId() );
            assertNotNull( messageInfo );
            assertEquals( messageInfo.getValue().getRedeliveryTime(), entry.getNextRedriveTime() );
            assertEquals( messageInfo.getKey(), entry.getInflightInfo() );
        }
        
        // Advance the time 
        time += 5;
        checkpoint = readerInflightData.getCheckpoint(-1);
        assertEquals( readerInflightData.getAckLevel(), checkpoint.getAckLevel() );
        assertEquals( readLevel, checkpoint.getReadLevel() );
        assertEquals( messageMap.size(), checkpoint.getInflightMessages().size() );
        
        for( CheckpointEntryInfo<AckIdV3, BasicInflightInfo> entry : checkpoint.getInflightMessages() ) {
            Pair<BasicInflightInfo, RedeliverySchedule> messageInfo = messageMap.get( entry.getAckId() );
            assertNotNull( messageInfo );
            assertEquals( messageInfo.getValue().getRedeliveryTime(), entry.getNextRedriveTime() );
            assertEquals( messageInfo.getKey(), entry.getInflightInfo() );
        }
        
        // Ack every other message
        {
            Iterator<AckIdV3> itr = messageMap.keySet().iterator();
            int messageNum = 0; 
            while( itr.hasNext() ) {
                AckIdV3 ackId = itr.next();
                if( messageNum % 2 == 0 ) {
                    readerInflightData.ack( ackId );
                    itr.remove();
                }
                messageNum++;
            }
        }
        
        checkpoint = readerInflightData.getCheckpoint(-1);
        assertEquals( readerInflightData.getAckLevel(), checkpoint.getAckLevel() );
        assertEquals( readLevel, checkpoint.getReadLevel() );
        assertEquals( messageMap.size(), checkpoint.getInflightMessages().size() );
        
        for( CheckpointEntryInfo<AckIdV3, BasicInflightInfo> entry : checkpoint.getInflightMessages() ) {
            Pair<BasicInflightInfo, RedeliverySchedule> messageInfo = messageMap.get( entry.getAckId() );
            assertNotNull( messageInfo );
            
            assertEquals( messageInfo.getValue().getRedeliveryTime(), entry.getNextRedriveTime() );
            assertEquals( messageInfo.getKey(), entry.getInflightInfo() );
        }
    }
    
    @Test
    public void testFullCheckpointRestore() {
        final int NUM_MESSAGES = 200;
        final BasicInflightInfo info1 = new BasicInflightInfo(1);
        final BasicInflightInfo info2 = new BasicInflightInfo(2);
        long time = 5;
        
        Checkpoint<AckIdV3, AckIdV3, BasicInflightInfo> checkpoint;
        AckIdGenerator idGenerator = new AckIdGenerator(time);
        ReaderInflightData<BasicInflightInfo> readerInflightData1 = new ReaderInflightData<BasicInflightInfo>();
        
        NavigableMap<AckIdV3, Pair<BasicInflightInfo, RedeliverySchedule>> messageMap = 
            new TreeMap<AckIdV3, Pair<BasicInflightInfo,RedeliverySchedule>>();
        
        addMessagesToInflight(messageMap, time, 10, NUM_MESSAGES / 2, idGenerator, info1, readerInflightData1 );
        addMessagesToInflight(messageMap, time + 2, 15, NUM_MESSAGES / 2, idGenerator, info2, readerInflightData1 );
        
        // Advance the time 
        time += 5;
        
        // Ack every other message
        {
            Iterator<AckIdV3> itr = messageMap.keySet().iterator();
            int messageNum = 0; 
            while( itr.hasNext() ) {
                AckIdV3 ackId = itr.next();
                if( messageNum % 2 == 0 ) {
                    readerInflightData1.ack( ackId );
                    itr.remove();
                }
                messageNum++;
            }
        }
        
        // Get a checkpoint
        checkpoint = readerInflightData1.getCheckpoint(-1);
        
        ReaderInflightData<BasicInflightInfo> readerInflightData2 = 
            new ReaderInflightData<BasicInflightInfo>(checkpoint);
        
        assertEquals( readerInflightData1.getAckLevel(), readerInflightData2.getAckLevel());
        assertEquals( readerInflightData1.getAllEntries(), readerInflightData2.getAllEntries() );
        
        for(;;) {
            Pair<RedeliverySchedule, BasicInflightInfo> nextMessage1 
                = readerInflightData1.takeFirstScheduled(Long.MAX_VALUE);
            Pair<RedeliverySchedule, BasicInflightInfo> nextMessage2 
                = readerInflightData2.takeFirstScheduled(Long.MAX_VALUE);
            
            assertEquals( nextMessage1, nextMessage2 );
            if( nextMessage1 == null ) break;
            readerInflightData1.ack( nextMessage1.getKey().getAckId() );
            readerInflightData2.ack( nextMessage2.getKey().getAckId() );
        }
        
        assertEquals( readerInflightData1.getAckLevel(), readerInflightData2.getAckLevel());
    }
    
    private static <T> T getNthElement( Collection<T> collection, int n) {
        if( collection.size() <= n ) throw new IndexOutOfBoundsException();
        Iterator<T> itr = collection.iterator();
        for( int i = 0; i < n - 1; ++i ) {
            itr.next();
        }
        return itr.next();
    }
    
    @Test
    public void testPartialCheckpointContents() {
        final int NUM_MESSAGES = 100;
        final BasicInflightInfo info1 = new BasicInflightInfo(1);
        final BasicInflightInfo info2 = new BasicInflightInfo(2);
        long time = 5;
        
        Checkpoint<AckIdV3, AckIdV3, BasicInflightInfo> checkpoint;
        AckIdGenerator idGenerator = new AckIdGenerator(time);
        ReaderInflightData<BasicInflightInfo> readerInflightData = new ReaderInflightData<BasicInflightInfo>();
        AckIdV3 readLevel; 
        
        NavigableMap<AckIdV3, Pair<BasicInflightInfo, RedeliverySchedule>> messageMap = 
            new TreeMap<AckIdV3, Pair<BasicInflightInfo,RedeliverySchedule>>();
        
        addMessagesToInflight(messageMap, time, 10, ( int ) ( 1.5 * NUM_MESSAGES ), idGenerator, info1, readerInflightData );
        addMessagesToInflight(messageMap, time + 2, 15, NUM_MESSAGES, idGenerator, info2, readerInflightData );
        readLevel = new AckIdV3( messageMap.lastKey(), true );
        
        checkpoint = readerInflightData.getCheckpoint(NUM_MESSAGES );
        assertEquals( readerInflightData.getAckLevel(), checkpoint.getAckLevel() );
        
        AckIdV3 checkpointReadLevel =  new AckIdV3( getNthElement(messageMap.keySet(), NUM_MESSAGES + 1), false );
        assertEquals( checkpointReadLevel, checkpoint.getReadLevel() );
        assertEquals( NUM_MESSAGES, checkpoint.getInflightMessages().size() );
        
        for( CheckpointEntryInfo<AckIdV3, BasicInflightInfo> entry : checkpoint.getInflightMessages() ) {
            assertTrue( checkpointReadLevel.compareTo( entry.getAckId() ) > 0 );
            
            Pair<BasicInflightInfo, RedeliverySchedule> messageInfo = messageMap.get( entry.getAckId() );
            assertNotNull( messageInfo );
            assertEquals( messageInfo.getValue().getRedeliveryTime(), entry.getNextRedriveTime() );
            assertEquals( messageInfo.getKey(), entry.getInflightInfo() );
        }
        
        // Ack every 4th message
        {
            Iterator<AckIdV3> itr = messageMap.keySet().iterator();
            int messageNum = 0; 
            while( itr.hasNext() ) {
                AckIdV3 ackId = itr.next();
                if( messageNum % 4 == 0 ) {
                    readerInflightData.ack( ackId );
                    itr.remove();
                }
                messageNum++;
            }
        }
        
        checkpoint = readerInflightData.getCheckpoint(NUM_MESSAGES );
        assertEquals( readerInflightData.getAckLevel(), checkpoint.getAckLevel() );
        checkpointReadLevel = new AckIdV3( getNthElement(messageMap.keySet(), NUM_MESSAGES + 1), false );
        assertEquals( checkpointReadLevel, checkpoint.getReadLevel() );
        assertEquals( NUM_MESSAGES, checkpoint.getInflightMessages().size() );
        
        for( CheckpointEntryInfo<AckIdV3, BasicInflightInfo> entry : checkpoint.getInflightMessages() ) {
            assertTrue( checkpointReadLevel.compareTo( entry.getAckId() ) > 0 );
            
            Pair<BasicInflightInfo, RedeliverySchedule> messageInfo = messageMap.get( entry.getAckId() );
            assertNotNull( messageInfo );
            assertEquals( messageInfo.getValue().getRedeliveryTime(), entry.getNextRedriveTime() );
            assertEquals( messageInfo.getKey(), entry.getInflightInfo() );
        }
        
        // Get a checkpoint with a minumum number of messages bigger than the number of messages inflight
        checkpoint = readerInflightData.getCheckpoint(NUM_MESSAGES * 4 );
        assertEquals( readerInflightData.getAckLevel(), checkpoint.getAckLevel() );
        assertEquals( readLevel, checkpoint.getReadLevel() );
        assertEquals( messageMap.size(), checkpoint.getInflightMessages().size() );
        
        for( CheckpointEntryInfo<AckIdV3, BasicInflightInfo> entry : checkpoint.getInflightMessages() ) {
            Pair<BasicInflightInfo, RedeliverySchedule> messageInfo = messageMap.get( entry.getAckId() );
            assertNotNull( messageInfo );
            assertEquals( messageInfo.getValue().getRedeliveryTime(), entry.getNextRedriveTime() );
            assertEquals( messageInfo.getKey(), entry.getInflightInfo() );
        }
    }
    
    @Test
    public void testPartialCheckpointRestore() {
        final int NUM_MESSAGES = 100;
        final BasicInflightInfo info1 = new BasicInflightInfo(1);
        final BasicInflightInfo info2 = new BasicInflightInfo(2);
        long time = 5;
        
        Checkpoint<AckIdV3, AckIdV3, BasicInflightInfo> checkpoint;
        AckIdGenerator idGenerator = new AckIdGenerator(time);
        ReaderInflightData<BasicInflightInfo> readerInflightData1 = new ReaderInflightData<BasicInflightInfo>();
        
        NavigableMap<AckIdV3, Pair<BasicInflightInfo, RedeliverySchedule>> messageMap = 
            new TreeMap<AckIdV3, Pair<BasicInflightInfo,RedeliverySchedule>>();
        
        addMessagesToInflight(messageMap, time, 10, ( int ) ( 1.5 * NUM_MESSAGES ), idGenerator, info1, readerInflightData1 );
        addMessagesToInflight(messageMap, time + 2, 15, NUM_MESSAGES, idGenerator, info2, readerInflightData1 );
        
        // Ack every 4th message
        {
            Iterator<AckIdV3> itr = messageMap.keySet().iterator();
            int messageNum = 0; 
            while( itr.hasNext() ) {
                AckIdV3 ackId = itr.next();
                if( messageNum % 4 == 0 ) {
                    readerInflightData1.ack( ackId );
                    itr.remove();
                }
                messageNum++;
            }
        }
        
        checkpoint = readerInflightData1.getCheckpoint(NUM_MESSAGES );
        ReaderInflightData<BasicInflightInfo> readerInflightData2 = 
            new ReaderInflightData<BasicInflightInfo>(checkpoint);
        
        assertEquals( readerInflightData1.getAckLevel(), readerInflightData2.getAckLevel());
        assertEquals( NUM_MESSAGES, readerInflightData2.getAllEntries().size() );
        
        for(int i = 0; i < NUM_MESSAGES; ++i ) {
            Pair<RedeliverySchedule, BasicInflightInfo> nextMessage1 
                = readerInflightData1.takeFirstScheduled(Long.MAX_VALUE);
            Pair<RedeliverySchedule, BasicInflightInfo> nextMessage2 
                = readerInflightData2.takeFirstScheduled(Long.MAX_VALUE);
            
            assertNotNull( nextMessage2 );
            assertEquals( nextMessage1, nextMessage2 );
            
            readerInflightData1.ack( nextMessage1.getKey().getAckId() );
            readerInflightData2.ack( nextMessage2.getKey().getAckId() );
        }
        
        assertNull( readerInflightData2.takeFirstScheduled(Long.MAX_VALUE) );
    }
    
    @Test
    public void testMergeWithEmptyInflight() {
        int time = 5;
        final int NUM_MESSAGES = 100;
        final AckIdGenerator idGenerator = new AckIdGenerator(time);
        
        ReaderInflightData<BasicInflightInfo> readerInflightData = new ReaderInflightData<BasicInflightInfo>();
        
        TreeSet<AckIdV3> messages = new TreeSet<AckIdV3>();
        for( int i = 0; i < NUM_MESSAGES; ++i ) {
            messages.add( idGenerator.next( time ) );
        }
        AckIdV3 ackLevel = new AckIdV3( messages.first(), false );
        AckIdV3 readLevel = new AckIdV3( messages.last(), true );
        
        readerInflightData.mergePartialCheckpoint(
                messages, ackLevel, readLevel, time );
        
        assertEquals( ackLevel, readerInflightData.getAckLevel() );
        
        List<Pair<RedeliverySchedule, BasicInflightInfo>> allEntries = readerInflightData.getAllEntries();
        assertEquals( messages.size(), allEntries.size() );
        
        for( Pair<RedeliverySchedule, BasicInflightInfo> entry : allEntries ) {
            assertTrue( messages.contains( entry.getKey().getAckId() ) );
            assertNull( entry.getValue() );
            assertEquals( time, entry.getKey().getRedeliveryTime() );
        }
    }
    
    @Test
    public void testMergeWithNewerCheckpoint() {
        long time = 5;
        final int NUM_MESSAGES = 200;
        final AckIdGenerator idGenerator = new AckIdGenerator(time);
        final BasicInflightInfo info1 = new BasicInflightInfo(1);
        final BasicInflightInfo info2 = new BasicInflightInfo(2);
        
        ReaderInflightData<BasicInflightInfo> readerInflightData = new ReaderInflightData<BasicInflightInfo>();
        
        NavigableMap<AckIdV3, Pair<BasicInflightInfo, RedeliverySchedule>> originalMessageMap = 
            new TreeMap<AckIdV3, Pair<BasicInflightInfo,RedeliverySchedule>>();
        
        addMessagesToInflight(originalMessageMap, time, 10, NUM_MESSAGES / 2, idGenerator, info1, readerInflightData );
        time += 2;
        addMessagesToInflight(originalMessageMap, time, 15, NUM_MESSAGES / 2, idGenerator, info2, readerInflightData );
        
        TreeSet<AckIdV3> checkpointMessages = new TreeSet<AckIdV3>();
        
        // Assume first 1/3 of the messages have been completely acked 
        AckIdV3 newAckLevel = new AckIdV3( getNthElement(originalMessageMap.keySet(), NUM_MESSAGES / 3 ), false );
        
        // Assume 1/4 of messages after that 1/3 have also been acked 
        {
            int messageNum = 0;
            for( AckIdV3 ackid : originalMessageMap.navigableKeySet().tailSet( newAckLevel, false) ) {
                if( messageNum % 4 == 0 ) checkpointMessages.add( ackid );
                messageNum++;
            }
        }
        
        // Time moves forward by 5
        time += 5;
        
        // And another NUM_MESSAGES / 2 have been added
        for( int i = 0; i < NUM_MESSAGES / 2; ++i ) {
            checkpointMessages.add( idGenerator.next( time ) );
        }
        
        AckIdV3 newReadLevel = new AckIdV3( checkpointMessages.last(), true );
        
        boolean outOfDate = readerInflightData.mergePartialCheckpoint(
                checkpointMessages, newAckLevel, newReadLevel, time );
        
        assertFalse( outOfDate );
        
        assertEquals( newAckLevel, readerInflightData.getAckLevel() );
        
        assertEquals( checkpointMessages.size(), readerInflightData.getAllEntries().size() );
        
        int messageCount = 0;
        for(;;) {
            Pair<RedeliverySchedule, BasicInflightInfo> nextMessage 
                = readerInflightData.takeFirstScheduled(Long.MAX_VALUE);
            if( nextMessage == null ) break;
            messageCount++;
            
            AckIdV3 ackId = nextMessage.getKey().getAckId();
            assertTrue( ackId + " not in checksumMessages", checkpointMessages.contains( ackId ) );
            
            Pair<BasicInflightInfo, RedeliverySchedule> originalValue = originalMessageMap.get( ackId );
            if( originalValue != null ) {
                assertEquals( originalValue.getValue(), nextMessage.getKey() );
                assertEquals( originalValue.getKey(), nextMessage.getValue() );
            } else {
                assertEquals( time, nextMessage.getKey().getRedeliveryTime() );
                assertNull( nextMessage.getValue() );
            }
            
            readerInflightData.ack( ackId );
        }
        
        assertEquals( checkpointMessages.size(), messageCount );
    }
    
    @Test
    public void testMergeWithOldCheckpoint() {
        long time = 5;
        final int NUM_MESSAGES = 200;
        final AckIdGenerator idGenerator = new AckIdGenerator(time);
        final BasicInflightInfo info1 = new BasicInflightInfo(1);
        final BasicInflightInfo info2 = new BasicInflightInfo(2);
        
        ReaderInflightData<BasicInflightInfo> readerInflightData = new ReaderInflightData<BasicInflightInfo>();
        
        NavigableMap<AckIdV3, Pair<BasicInflightInfo, RedeliverySchedule>> messageMap = 
            new TreeMap<AckIdV3, Pair<BasicInflightInfo,RedeliverySchedule>>();
        
        addMessagesToInflight(messageMap, time, 10, NUM_MESSAGES / 2, idGenerator, info1, readerInflightData );
        
        Set<AckIdV3> checkpointMessages = new HashSet<AckIdV3>( messageMap.keySet() );
        AckIdV3 oldAcklevel = readerInflightData.getAckLevel();
        AckIdV3 oldReadLevel =  new AckIdV3( messageMap.lastKey(), true );
        
        time += 2;
        addMessagesToInflight(messageMap, time, 15, NUM_MESSAGES / 2, idGenerator, info2, readerInflightData );
        
        // Ack first 1/3 of the messages have been completely acked
        Iterator<AckIdV3> itr = messageMap.keySet().iterator();
        for( int cnt = 0; cnt < NUM_MESSAGES / 3; ++cnt ) {
            readerInflightData.ack( itr.next() );
            itr.remove();
        }
        
        // Assume 1/4 of messages after that have also been acked 
        {
            int messageNum = 0;
            while( itr.hasNext() ) {
                AckIdV3 ackId = itr.next();
                if( messageNum % 4 == 0 ) {
                    readerInflightData.ack( ackId );
                    itr.remove();
                }
                messageNum++;
            }
        }
        
        boolean outOfDate = readerInflightData.mergePartialCheckpoint(
                checkpointMessages, oldAcklevel, oldReadLevel, time );
        assertTrue( outOfDate );
        
        Iterator<Pair<BasicInflightInfo, RedeliverySchedule>> inflightItr = messageMap.values().iterator();
        for(;;) {
            Pair<RedeliverySchedule, BasicInflightInfo> nextMessage 
                = readerInflightData.takeFirstScheduled(Long.MAX_VALUE);
            if( nextMessage == null ) {
                if( inflightItr.hasNext() ) {
                    fail( "Ran out of messages while still expecting " + inflightItr.next().getValue().getAckId() + " to be available." );
                }
                break;
            }
            assertTrue( "Found extra message " + nextMessage, inflightItr.hasNext() );
            Pair<BasicInflightInfo, RedeliverySchedule> expected = inflightItr.next(); 
            
            assertEquals( expected.getValue(), nextMessage.getKey() );
            assertEquals( expected.getKey(), nextMessage.getValue() );
            readerInflightData.ack( expected.getValue().getAckId() );
        }
    }
    
    
    @Test
    public void testMergeWithUnchangedCheckpoint() {
        long time = 5;
        final int NUM_MESSAGES = 200;
        final AckIdGenerator idGenerator = new AckIdGenerator(time);
        final BasicInflightInfo info1 = new BasicInflightInfo(1);
        final BasicInflightInfo info2 = new BasicInflightInfo(2);
        
        ReaderInflightData<BasicInflightInfo> readerInflightData = new ReaderInflightData<BasicInflightInfo>();
        
        NavigableMap<AckIdV3, Pair<BasicInflightInfo, RedeliverySchedule>> originalMessageMap = 
            new TreeMap<AckIdV3, Pair<BasicInflightInfo,RedeliverySchedule>>();
        
        addMessagesToInflight(originalMessageMap, time, 10, NUM_MESSAGES / 2, idGenerator, info1, readerInflightData );
        time += 2;
        addMessagesToInflight(originalMessageMap, time, 15, NUM_MESSAGES / 2, idGenerator, info2, readerInflightData );
        
        boolean outOfDate = readerInflightData.mergePartialCheckpoint(
                originalMessageMap.keySet(),  
                new AckIdV3( originalMessageMap.firstKey(), false ),
                new AckIdV3( originalMessageMap.lastKey(), true ), time );
        
        assertFalse( outOfDate );
        
        assertEquals( new AckIdV3( originalMessageMap.firstKey(), false ), readerInflightData.getAckLevel() );
        
        assertEquals( originalMessageMap.size(), readerInflightData.getAllEntries().size() );
        
        int messageCount = 0;
        for(;;) {
            Pair<RedeliverySchedule, BasicInflightInfo> nextMessage 
                = readerInflightData.takeFirstScheduled(Long.MAX_VALUE);
            if( nextMessage == null ) break;
            messageCount++;
            
            AckIdV3 ackId = nextMessage.getKey().getAckId();
            assertTrue( ackId + " not in checksumMessages", originalMessageMap.containsKey( ackId ) );
            
            Pair<BasicInflightInfo, RedeliverySchedule> originalValue = originalMessageMap.get( ackId );
            if( originalValue != null ) {
                assertEquals( originalValue.getValue(), nextMessage.getKey() );
                assertEquals( originalValue.getKey(), nextMessage.getValue() );
            } else {
                assertEquals( time, nextMessage.getKey().getRedeliveryTime() );
                assertNull( nextMessage.getValue() );
            }
            
            readerInflightData.ack( ackId );
        }
        
        assertEquals( originalMessageMap.size(), messageCount );
    }
    
    @Test
    public void testMergeWithInconsistentCheckpoint() {
        final long initialTime = 5;
        long time = initialTime;
        final int NUM_MESSAGES = 200;
        final AckIdGenerator idGenerator = new AckIdGenerator(initialTime);
        final BasicInflightInfo info1 = new BasicInflightInfo(1);
        final BasicInflightInfo info2 = new BasicInflightInfo(2);
        
        ReaderInflightData<BasicInflightInfo> readerInflightData = new ReaderInflightData<BasicInflightInfo>();
        
        NavigableMap<AckIdV3, Pair<BasicInflightInfo, RedeliverySchedule>> messageMap = 
            new TreeMap<AckIdV3, Pair<BasicInflightInfo,RedeliverySchedule>>();
        
        addMessagesToInflight(messageMap, time, 10, NUM_MESSAGES / 2, idGenerator, info1, readerInflightData );
        
        time += 2;
        addMessagesToInflight(messageMap, time, 15, NUM_MESSAGES / 2, idGenerator, info2, readerInflightData );
        
        // Ack 1/4 of messages
        {
            int messageNum = 0;
            Iterator<AckIdV3> itr = messageMap.keySet().iterator();
            while( itr.hasNext() ) {
                AckIdV3 ackId = itr.next();
                if( messageNum % 4 == 0 ) {
                    readerInflightData.ack( ackId );
                    itr.remove();
                }
                messageNum++;
            }
        }
        
        AckIdGenerator idGenerator2 = new AckIdGenerator(idGenerator);
        NavigableSet<AckIdV3> checkpointMessages = new TreeSet<AckIdV3>( messageMap.keySet() );
        HashSet<AckIdV3> checkpointAckedMessages = new HashSet<AckIdV3>();
        
        // Ack 1/4 of remaining messages in the checkpoint
        {
            int messageNum = 0;
            Iterator<AckIdV3> itr = checkpointMessages.iterator();
            while( itr.hasNext() ) {
                AckIdV3 ackId = itr.next();
                if( messageNum % 4 == 0 ) {
                    itr.remove();
                    checkpointAckedMessages.add( ackId );
                }
                messageNum++;
            }
        }
        
        HashSet<AckIdV3> checkpointAddedMessages = new HashSet<AckIdV3>();
        // And a few new ones
        for( int i = 0; i < NUM_MESSAGES / 2; ++i ) {
            AckIdV3 newAckId = idGenerator2.next( time );
            checkpointMessages.add( newAckId );
            checkpointAddedMessages.add( newAckId );
        }
        
        // Ack a different 1/4 of remaining messages in the readerInflightData
        {
            int messageNum = 0;
            Iterator<AckIdV3> itr = messageMap.keySet().iterator();
            while( itr.hasNext() ) {
                AckIdV3 ackId = itr.next();
                if( messageNum % 4 == 1 ) {
                    readerInflightData.ack( ackId );
                    itr.remove();
                }
                messageNum++;
            }
        }
        
        // And a few smaller set of new ones
        for( int i = 0; i < NUM_MESSAGES / 4; ++i ) {
            checkpointMessages.add( idGenerator.next( time ) );
        }
        
        boolean outOfDate = readerInflightData.mergePartialCheckpoint(
                checkpointMessages, 
                new AckIdV3( checkpointMessages.first(), false), 
                new AckIdV3( checkpointMessages.last(), true),
                time );
        assertTrue( outOfDate );
        
        messageMap.keySet().removeAll( checkpointAckedMessages );
        for( AckIdV3 newAckId : checkpointAddedMessages ) {
            if( !messageMap.containsKey( newAckId ) ) {
                messageMap.put( newAckId, 
                        new Pair<BasicInflightInfo, RedeliverySchedule>(
                                null, new RedeliverySchedule( newAckId, time )));
            }
        }
        
        int messageCount = 0;
        for(;;) {
            Pair<RedeliverySchedule, BasicInflightInfo> nextMessage 
                = readerInflightData.takeFirstScheduled(Long.MAX_VALUE);
            if( nextMessage == null ) break;
            messageCount++;
            
            AckIdV3 ackId = nextMessage.getKey().getAckId();
            assertTrue( ackId + " not in checksumMessages", checkpointMessages.contains( ackId ) );
            
            Pair<BasicInflightInfo, RedeliverySchedule> expectedValue = messageMap.get( ackId );
            assertEquals( expectedValue.getValue(), nextMessage.getKey() );
            assertEquals( expectedValue.getKey(), nextMessage.getValue() );
            
            readerInflightData.ack( ackId );
        }
        
        assertEquals( messageMap.size(), messageCount );
    }
    
    private void addMessagesToInflight(Map<AckIdV3, Pair<BasicInflightInfo, RedeliverySchedule>> messageMap,
                                       long time, long redeliveryTime, int count, 
                                       AckIdGenerator generator, BasicInflightInfo info,
                                       ReaderInflightData<BasicInflightInfo> inflight)
    {
        for( int i = 0; i < count; ++i ) {
            AckIdV3 message = generator.next( time );
            messageMap.put( message, 
                    new Pair<BasicInflightInfo, RedeliverySchedule>( 
                            info, new RedeliverySchedule(message, redeliveryTime)));
            inflight.add( message, redeliveryTime, info );
        }
    }
}
