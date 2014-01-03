package com.amazon.coral.spring.director.test.sample;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.coral.spring.director.live.LiveAdapter;
import com.amazon.coral.tally.Notch;
import com.amazon.coral.tally.Tally;

public class WorkerC extends LiveAdapter {
    private final static Log log = LogFactory.getLog(WorkerC.class);
    private boolean failOnActivate = false;

    private Tally eventLog;
    private int sleep;

    public void setEventLog(Tally eventLog) {
        this.eventLog = eventLog;
    }

    public WorkerC(Tally eventLog) {
        this.eventLog = eventLog;

        log.debug("constructor");
        eventLog.add(new Notch(this, "constructor"));

        sleep = 0;
    }

    public void setFailOnActivate(boolean value) {
        this.failOnActivate = value;
    }

    @Override
    public void acquire() throws Exception {
        eventLog.add(new Notch(this, "acquire", "begin"));
        log.debug("    worker C: acquire");
        eventLog.add(new Notch(this, "acquire", "end"));
    }

    @Override
    public void activate() throws Exception {
        eventLog.add(new Notch(this, "activate", "begin"));

        log.debug("    worker C: activate work");

        Thread.sleep(sleep);

        if (failOnActivate) {
            log.debug("    worker C: activate failed, will throw exception");
            throw new Exception("Failed to activate");
        }

        eventLog.add(new Notch(this, "activate", "end"));
    }

    @Override
    public void deactivate() throws Exception {
        eventLog.add(new Notch(this, "deactivate", "begin"));

        log.debug("    worker C: deactivate");

        eventLog.add(new Notch(this, "deactivate", "end"));
    }

    @Override
    public void release() throws Exception {
        eventLog.add(new Notch(this, "release", "begin"));

        log.debug("    worker C: release");

        eventLog.add(new Notch(this, "release", "end"));
    }

    public void setSleep(int sleep) {
        this.sleep = sleep;
    }
}
