package com.amazon.coral.spring.director.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.springframework.context.support.AbstractApplicationContext;

import com.amazon.coral.spring.director.test.sample.WorkerA;
import com.amazon.coral.spring.director.test.support.ThreeBeansModel;
import com.amazon.coral.spring.director.test.util.SpringUtil;

public class AForkJoinChainTest {
    private final static Log log = LogFactory.getLog(WorkerA.class);

    @Test
    public void testA() {
        log.debug("TestA");

        AbstractApplicationContext context = SpringUtil.getContext("config/beans-A-fj-chain.xml");

        ThreeBeansModel tbm = new ThreeBeansModel(context);
        tbm.correctnessTest();
    }
}
