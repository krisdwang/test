package com.amazon.coral.spring.director.test.sample;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BobcatMockup {
    private final static Log log = LogFactory.getLog(BobcatMockup.class);
    private String config;

    public void setConfig(String config) {
        this.config = config;
    }

    public void setMode(String passive) {
    }

    public void mockAcquire() {
        log.debug(String.format("    bobcat %s: initializing...", config));
        try {
            Thread.sleep(750);
        } catch (InterruptedException e) {
        }
        log.debug(String.format("    bobcat %s: initialization complete", config));
    }

    public void mockActivate() {
        log.debug(String.format("    bobcat %s: listening on 80", config));
    }

    public void mockDeactivate() {
        log.debug(String.format("    bobcat %s: stopped listening on 80", config));
    }

    public void mockRelease() {
        log.debug(String.format("    bobcat %s: entering graceful shutdown period", config));
        try {
            Thread.sleep(350);
        } catch (InterruptedException e) {
        }
        log.debug(String.format("    bobcat %s: graceful shutdown period ended", config));
    }

}
