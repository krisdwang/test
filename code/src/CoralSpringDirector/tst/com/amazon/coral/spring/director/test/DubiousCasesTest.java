package com.amazon.coral.spring.director.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.springframework.context.ApplicationContextException;
import org.springframework.context.support.AbstractApplicationContext;

import com.amazon.coral.spring.director.test.util.SpringUtil;

public class DubiousCasesTest {
    private final static Log log = LogFactory.getLog(DubiousCasesTest.class);

    @Test(expected = ApplicationContextException.class)
    public void testA() {
        log.debug("testA");

        AbstractApplicationContext context = SpringUtil.getContext("config/beans-dubious-1.xml");
    }

    @Test(expected = ApplicationContextException.class)
    public void testB() {
        log.debug("testB");

        AbstractApplicationContext context = SpringUtil.getContext("config/beans-dubious-2.xml");
    }

    @Test
    public void testC() {
        log.debug("testC");

        AbstractApplicationContext context = SpringUtil.getContext("config/beans-dubious-ignored.xml");
    }
}
