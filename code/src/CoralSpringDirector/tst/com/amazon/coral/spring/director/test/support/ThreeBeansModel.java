package com.amazon.coral.spring.director.test.support;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.support.AbstractApplicationContext;

import com.amazon.coral.spring.director.Plan;
import com.amazon.coral.spring.director.event.NamedEvent;
import com.amazon.coral.spring.director.fj.ForkJoin;
import com.amazon.coral.spring.director.fj.ForkJoinChain;
import com.amazon.coral.spring.director.live.LiveEvent;
import com.amazon.coral.spring.director.test.sample.WorkerA;
import com.amazon.coral.spring.director.test.sample.WorkerB;
import com.amazon.coral.spring.director.test.sample.WorkerC;
import com.amazon.coral.tally.Notch;
import com.amazon.coral.tally.Tally;
import com.amazon.coral.tally.Tally.IndexedObject;

public class ThreeBeansModel {
    private final static Log log = LogFactory.getLog(ThreeBeansModel.class);

    public Tally tally;
    public WorkerA workerA;
    public WorkerB workerB;
    public WorkerC workerC;
    public Plan plan;
    public ForkJoinChain startupForkJoinChain;
    public ForkJoinChain shutdownForkJoinChain;

    public AbstractApplicationContext context;

    private final ForkJoin b1;

    private final ForkJoin b2;

    private final ForkJoin b3;

    private final ForkJoin b4;

    public ThreeBeansModel(AbstractApplicationContext context) {
        this.context = context;

        tally = context.getBean("tally", Tally.class);
        workerA = context.getBean("workerA", WorkerA.class);
        workerB = context.getBean("workerB", WorkerB.class);
        workerC = context.getBean("workerC", WorkerC.class);

        plan = context.getBean("plan", Plan.class);
        startupForkJoinChain = (ForkJoinChain) plan.getStartup().iterator().next();
        shutdownForkJoinChain = (ForkJoinChain) plan.getShutdown().iterator().next();

        context.close();

        b1 = startupForkJoinChain.getForkJoinsAsArray()[0];
        b2 = startupForkJoinChain.getForkJoinsAsArray()[1];

        b3 = shutdownForkJoinChain.getForkJoinsAsArray()[0];
        b4 = shutdownForkJoinChain.getForkJoinsAsArray()[1];
    }

    public void correctnessTest() {
        for (IndexedObject io : tally.getIndexedEvents()) {
            log.info(io);
        }

        assertEquals(startupForkJoinChain.getTick(), b1.getTick());
        assertEquals(startupForkJoinChain.getExceptionsOnJoinStrategy(), b1.getExceptionsOnJoinStrategy());
        assertEquals(startupForkJoinChain.getDubiousConfigStrategy(), b1.getDubiousConfigStrategy());
        assertEquals(startupForkJoinChain.getTargets(), b1.getTargets());
        assertEquals(startupForkJoinChain.getTargets(), b2.getTargets());
        assertEquals(startupForkJoinChain.getListener(), b1.getListener());
        assertEquals(startupForkJoinChain.getListener(), b2.getListener());

        NamedEvent acquire = new NamedEvent(startupForkJoinChain, LiveEvent.acquire.toString());
        NamedEvent activate = new NamedEvent(startupForkJoinChain, LiveEvent.activate.toString());

        NamedEvent deactivate = new NamedEvent(shutdownForkJoinChain, LiveEvent.deactivate.toString());
        NamedEvent release = new NamedEvent(shutdownForkJoinChain, LiveEvent.release.toString());

        assertTrue(!tally.isEmpty());

        assertTrue(tally.happened(new Notch(b1, acquire, "begin")));

        assertTrue(tally.inSequence(
                new Notch(b1, acquire, "begin"),
                new Notch(workerA, acquire, "begin"),
                new Notch(workerA, acquire, "end"),
                new Notch(b1, acquire, "end")));
        assertTrue(tally.inSequence(
                new Notch(b1, acquire, "begin"),
                new Notch(workerB, acquire, "begin"),
                new Notch(workerB, acquire, "end"),
                new Notch(b1, acquire, "end")));
        assertTrue(tally.inSequence(
                new Notch(b1, acquire, "begin"),
                new Notch(workerC, LiveEvent.acquire.toString(), "begin"),
                new Notch(workerC, LiveEvent.acquire.toString(), "end"),
                new Notch(b1, acquire, "end")));

        assertTrue(tally.inSequence(
                new Notch(b1, acquire, "end"),
                new Notch(b2, activate, "begin")));

        assertTrue(tally.inSequence(
                new Notch(b2, activate, "begin"),
                new Notch(workerA, activate, "begin"),
                new Notch(workerA, activate, "end"),
                new Notch(b2, activate, "end")));
        assertTrue(tally.inSequence(
                new Notch(b2, activate, "begin"),
                new Notch(workerB, activate, "begin"),
                new Notch(workerB, activate, "end"),
                new Notch(b2, activate, "end")));
        assertTrue(tally.inSequence(
                new Notch(b2, activate, "begin"),
                new Notch(workerC, LiveEvent.activate.toString(), "begin"),
                new Notch(workerC, LiveEvent.activate.toString(), "end"),
                new Notch(b2, activate, "end")));

        assertTrue(tally.inSequence(
                new Notch(b2, activate, "end"),
                new Notch(b3, deactivate, "begin"),
                new Notch(b3, deactivate, "end"),
                new Notch(b4, release, "begin"),
                new Notch(b4, release, "end")));
    }
}
