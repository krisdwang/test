package com.amazon.coral.spring.director.test;

import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.springframework.context.support.AbstractApplicationContext;

import com.amazon.coral.spring.director.test.util.SpringUtil;
import com.amazon.coral.tally.Tally;

public class EdgeCasesTest {
    private final static Log log = LogFactory.getLog(EdgeCasesTest.class);

    @Test
    public void testAutoStartupOff() {
        log.debug("testAutoStartupOff");

        AbstractApplicationContext context = SpringUtil.getContext("config/beans-plan-autoStartup-off.xml");

        Tally tally = context.getBean("tally", Tally.class);

        assertTrue(tally.getIndexedEvents().size() == 2);
    }

    @Test
    public void testDoNothing() {
        log.debug("testDoNothing");

        AbstractApplicationContext context = SpringUtil.getContext("config/beans-donothing.xml");
    }

}
