package com.amazon.coral.spring.director.test;

import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.springframework.context.support.AbstractApplicationContext;

import com.amazon.coral.spring.director.Plan;
import com.amazon.coral.spring.director.event.NamedEvent;
import com.amazon.coral.spring.director.fj.ForkJoin;
import com.amazon.coral.spring.director.live.LiveEvent;
import com.amazon.coral.spring.director.test.sample.WorkerA;
import com.amazon.coral.spring.director.test.sample.WorkerB;
import com.amazon.coral.spring.director.test.util.SpringUtil;
import com.amazon.coral.tally.Notch;
import com.amazon.coral.tally.Tally;

public class AForkJoinWithTimeTest {
    private final static Log log = LogFactory.getLog(WorkerA.class);

    @Test
    public void testA() {
        log.debug("TestA");

        AbstractApplicationContext context = SpringUtil.getContext("config/beans-A-fj-with-time.xml");

        Tally tally = context.getBean("tally", Tally.class);
        WorkerA workerA = context.getBean("workerA", WorkerA.class);
        WorkerB workerB = context.getBean("workerB", WorkerB.class);

        Plan p = context.getBean("plan", Plan.class);
        ForkJoin fj1 = (ForkJoin) p.getStartup().iterator().next();

        context.close();

        /*
        for (IndexedObject io : tally.getIndexedEvents()) {
            log.info(io);
        }
        */

        NamedEvent acquireEvent = new NamedEvent(fj1, LiveEvent.acquire.toString());

        assertTrue(!tally.isEmpty());
        assertTrue(tally.happened(new Notch(fj1, acquireEvent, "begin")));

        assertTrue(tally.inSequence(
                new Notch(fj1, acquireEvent, "begin"),
                new Notch(workerA, acquireEvent, "begin"),
                new Notch(fj1, acquireEvent, "tick"),
                new Notch(workerA, acquireEvent, "end"),
                new Notch(fj1, acquireEvent, "tick"),
                new Notch(workerB, acquireEvent, "end"),
                new Notch(fj1, acquireEvent, "end")));
        assertTrue(tally.inSequence(
                new Notch(fj1, acquireEvent, "begin"),
                new Notch(workerB, acquireEvent, "begin"),
                new Notch(workerB, acquireEvent, "end"),
                new Notch(fj1, acquireEvent, "end")));
    }
}
