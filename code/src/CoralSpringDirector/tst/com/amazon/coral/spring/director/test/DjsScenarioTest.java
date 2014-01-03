package com.amazon.coral.spring.director.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.context.support.AbstractApplicationContext;

import com.amazon.coral.spring.director.test.sample.WorkerA;
import com.amazon.coral.spring.director.test.util.SpringUtil;

@Ignore
public class DjsScenarioTest {
    private final static Log log = LogFactory.getLog(WorkerA.class);

    @Ignore
    @Test
    public void testA() {
        log.debug("TestA");

        AbstractApplicationContext context = SpringUtil.getContext("config/beans-DJS-scenario.xml");

        context.getBean("workerA");

        context.close();
    }
}
