package com.amazon.coral.spring.director.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.springframework.context.ApplicationContextException;
import org.springframework.context.support.AbstractApplicationContext;

import com.amazon.coral.spring.director.test.sample.WorkerA;
import com.amazon.coral.spring.director.test.util.SpringUtil;

public class AForkJoinTimeoutTest {
    private final static Log log = LogFactory.getLog(WorkerA.class);

    @Test(expected = ApplicationContextException.class)
    public void testA() {
        log.debug("TestA");

        AbstractApplicationContext context = SpringUtil.getContext("config/beans-A-fj-timeout.xml");
    }

    @Test(expected = ApplicationContextException.class)
    public void testB() {
        log.debug("TestB");

        AbstractApplicationContext context = SpringUtil.getContext("config/beans-A-fj-chain-timeout.xml");
    }

}
