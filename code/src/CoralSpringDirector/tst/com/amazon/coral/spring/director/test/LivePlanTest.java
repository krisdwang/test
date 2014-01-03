package com.amazon.coral.spring.director.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.support.AbstractApplicationContext;

import com.amazon.coral.spring.director.fj.ForkJoin;
import com.amazon.coral.spring.director.fj.ForkJoinChain;
import com.amazon.coral.spring.director.live.LivePlan;
import com.amazon.coral.spring.director.test.sample.WorkerA;
import com.amazon.coral.spring.director.test.support.ThreeBeansModel;
import com.amazon.coral.spring.director.test.util.SpringUtil;

public class LivePlanTest {
    private final static Log log = LogFactory.getLog(WorkerA.class);

    /**
     * Copied from AForkJoinChainTest.testA(), the corresponding XML-s should be equivalent.
     */
    @Test
    public void testA() {
        log.debug("TestA");

        AbstractApplicationContext context = SpringUtil.getContext("config/beans-live-plan.xml");

        ThreeBeansModel tbm = new ThreeBeansModel(context);
        tbm.correctnessTest();

        LivePlan livePlan = (LivePlan) tbm.plan;

        testStartupSettingsPropagation(livePlan, tbm.startupForkJoinChain);
        testShutdownSettingsPropagation(livePlan, tbm.shutdownForkJoinChain);
    }

    @Test
    public void testCustomized() {
        log.debug("testCustomized");

        AbstractApplicationContext context = SpringUtil.getContext("config/beans-live-plan-customized.xml");

        ThreeBeansModel tbm = new ThreeBeansModel(context);
        tbm.correctnessTest();

        LivePlan livePlan = (LivePlan) tbm.plan;

        testStartupSettingsPropagation(livePlan, tbm.startupForkJoinChain);
        testShutdownSettingsPropagation(livePlan, tbm.shutdownForkJoinChain);

        assertTrue(livePlan.getTargets().size() == 3);
    }

    @Test
    public void testExplicitOverridesForBackwardsCompatibility() {
        log.debug("testExplicitOverridesForBackwardsCompatibility");

        AbstractApplicationContext context = SpringUtil.getContext("config/beans-live-plan-explicit.xml");

        ThreeBeansModel tbm = new ThreeBeansModel(context);
        tbm.correctnessTest();

        LivePlan livePlan = (LivePlan) tbm.plan;

        assertTrue(livePlan.getTargets().size() == 1);

        for (ForkJoin b : tbm.startupForkJoinChain.getForkJoinsAsArray()) {
            assertTrue(b.getTick() != livePlan.getStartupTick());
            assertEquals(b.getTargets().size(), 3);
        }
        for (ForkJoin b : tbm.startupForkJoinChain.getForkJoinsAsArray()) {
            assertTrue(b.getTick() != livePlan.getShutdownTick());
            assertEquals(b.getTargets().size(), 3);
        }
    }

    @Test
    public void testSetSingleTarget() {
        ApplicationListener<ApplicationEvent> bean = new ApplicationListener<ApplicationEvent>() {
            @Override
            public void onApplicationEvent(ApplicationEvent event) {
            }
        };

        LivePlan p = new LivePlan();
        p.setTarget(bean);

        assertEquals(p.getTargets().size(), 1);
        assertEquals(p.getTargets().iterator().next(), bean);
    }

    @Test
    public void testPlainTarget() {
        testPlain(true);
        testPlain(false);
    }

    private void testPlain(boolean plural) {
        final byte callMask[] = new byte[1];
        callMask[0] = 0;

        Object bean = new Object() {
            public void acquire() {
                callMask[0] |= 0x01;
            }

            public void activate() {
                callMask[0] |= 0x02;
            }

            public void deactivate() {
                callMask[0] |= 0x04;
            }

            public void release() {
                callMask[0] |= 0x08;
            }
        };

        LivePlan livePlan = new LivePlan();
        if (plural) {
            LinkedList<Object> list = new LinkedList<Object>();
            list.add(bean);
            livePlan.setPlainTargets(list);
        } else {
            livePlan.setPlainTarget(bean);
        }

        livePlan.start();
        assertTrue(callMask[0] == 0x03);

        livePlan.stop(new Runnable() {
            @Override
            public void run() {
            }
        });

        assertTrue(callMask[0] == 0x0F);
    }

    private void testStartupSettingsPropagation(LivePlan livePlan, ForkJoinChain mb) {
        for (ForkJoin b : mb.getForkJoinsAsArray()) {
            assertTrue(livePlan.getStartupListener() == b.getListener());
            assertTrue(livePlan.getStartupDubiousConfigStrategy() == b.getDubiousConfigStrategy());
            assertTrue(livePlan.getStartupExceptionsOnJoinStrategy() == b.getExceptionsOnJoinStrategy());
            assertTrue(livePlan.getStartupTimeout() == b.getTimeout());
            assertTrue(livePlan.getStartupTick() == b.getTick());
            assertTrue(livePlan.getTargets().equals(b.getTargets()));
        }
    }

    private void testShutdownSettingsPropagation(LivePlan livePlan, ForkJoinChain mb) {
        assertTrue(livePlan.getShutdownListener() == mb.getListener());
        for (ForkJoin b : mb.getForkJoinsAsArray()) {
            assertTrue(livePlan.getShutdownListener() == b.getListener());
            assertTrue(livePlan.getShutdownDubiousConfigStrategy() == b.getDubiousConfigStrategy());
            assertTrue(livePlan.getShutdownExceptionsOnJoinStrategy() == b.getExceptionsOnJoinStrategy());
            assertTrue(livePlan.getShutdownTimeout() == b.getTimeout());
            assertTrue(livePlan.getShutdownTick() == b.getTick());
            assertTrue(livePlan.getTargets().equals(b.getTargets()));
        }
    }

}
