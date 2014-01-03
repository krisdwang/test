package com.amazon.coral.spring.director.test.sample;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationListener;

import com.amazon.coral.spring.director.event.NamedEvent;
import com.amazon.coral.tally.Notch;
import com.amazon.coral.tally.Tally;

public class WorkerA implements ApplicationListener<NamedEvent> {
    private final static Log log = LogFactory.getLog(WorkerA.class);

    private final Tally eventLog;

    private int iterations;

    private int sleep;

    public WorkerA(Tally eventLog) {
        this.eventLog = eventLog;

        log.debug("constructor");
        eventLog.add(new Notch(this, "constructor"));

        iterations = 0;
        sleep = 250;
    }

    @Override
    public void onApplicationEvent(NamedEvent event) {
        eventLog.add(new Notch(this, event, "begin"));

        for (int i = 0; i < iterations; i++) {
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
            }
            log.debug("    worker A: working to" + event.toString().replace('{', ' ').replace('}', ' '));
        }

        eventLog.add(new Notch(this, event, "end"));
    }

    public void setIterations(int iterations) {
        this.iterations = iterations;
    }

    public void setSleep(int sleep) {
        this.sleep = sleep;
    }
}
