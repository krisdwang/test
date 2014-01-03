package com.amazon.coral.spring.director.test;

import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.springframework.context.support.AbstractApplicationContext;

import com.amazon.coral.spring.director.Plan;
import com.amazon.coral.spring.director.event.NamedEvent;
import com.amazon.coral.spring.director.fj.ForkJoin;
import com.amazon.coral.spring.director.fj.ForkJoinChain;
import com.amazon.coral.spring.director.live.LiveEvent;
import com.amazon.coral.spring.director.test.sample.WorkerA;
import com.amazon.coral.spring.director.test.util.SpringUtil;
import com.amazon.coral.tally.Notch;
import com.amazon.coral.tally.Tally;
import com.amazon.coral.tally.Tally.IndexedObject;

public class LiveDelegatorTest {
    private final static Log log = LogFactory.getLog(WorkerA.class);

    @Test
    public void testA() {
        log.debug("TestA");

        AbstractApplicationContext context = SpringUtil.getContext("config/beans-live-delegator.xml");

        Tally tally = context.getBean("tally", Tally.class);

        Plan p = context.getBean("plan", Plan.class);
        ForkJoinChain fjc1 = (ForkJoinChain) p.getStartup().iterator().next();
        ForkJoinChain fjc2 = (ForkJoinChain) p.getShutdown().iterator().next();

        context.close();

        ForkJoin fj11 = fjc1.getForkJoinsAsArray()[0];
        ForkJoin fj12 = fjc1.getForkJoinsAsArray()[1];

        ForkJoin fj21 = fjc2.getForkJoinsAsArray()[0];
        ForkJoin fj22 = fjc2.getForkJoinsAsArray()[1];

        NamedEvent acquire = new NamedEvent(fjc1, LiveEvent.acquire.toString());
        NamedEvent activate = new NamedEvent(fjc1, LiveEvent.activate.toString());
        NamedEvent deactivate = new NamedEvent(fjc2, LiveEvent.deactivate.toString());
        NamedEvent release = new NamedEvent(fjc2, LiveEvent.release.toString());

        assertTrue(!tally.isEmpty());

        for (IndexedObject io : tally.getIndexedEvents()) {
            log.info(io);
        }

        assertTrue(tally.inSequence(
                new Notch(fj11, acquire, "begin"),
                "pure live acquire",
                new Notch(fj11, acquire, "end")));
        assertTrue(tally.inSequence(
                new Notch(fj12, activate, "begin"),
                "pure live activate",
                new Notch(fj12, activate, "end")));
        assertTrue(tally.inSequence(
                new Notch(fj21, deactivate, "begin"),
                "pure live deactivate",
                new Notch(fj21, deactivate, "end")));
        assertTrue(tally.inSequence(
                new Notch(fj22, release, "begin"),
                "pure live release",
                new Notch(fj22, release, "end")));

        assertTrue(tally.inSequence(
                new Notch(fj11, acquire, "begin"),
                new Notch(fj11, acquire, "end"),
                new Notch(fj12, activate, "begin"),
                new Notch(fj12, activate, "end"),
                new Notch(fj21, deactivate, "begin"),
                new Notch(fj21, deactivate, "end"),
                new Notch(fj22, release, "begin"),
                new Notch(fj22, release, "end")));
    }
}
