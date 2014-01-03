package com.amazon.coral.spring.director.test.sample;

import org.springframework.context.ApplicationEvent;

import com.amazon.coral.spring.director.fj.ForkJoin;
import com.amazon.coral.spring.director.fj.ListenerLogger;
import com.amazon.coral.tally.Notch;
import com.amazon.coral.tally.Tally;

public class ListenerLoggerPlus extends ListenerLogger {

    private final Tally tally;

    public ListenerLoggerPlus(Tally tally) {
        this.tally = tally;
    }

    @Override
    public void onBegin(ForkJoin b, ApplicationEvent event) {
        super.onBegin(b, event);

        tally.add(new Notch(b, event, "begin"));
    }

    @Override
    public void onEnd(ForkJoin b, ApplicationEvent event) {
        super.onEnd(b, event);

        tally.add(new Notch(b, event, "end"));
    }

    @Override
    public void onTick(ForkJoin b, ApplicationEvent event, long timeSinceStart) {
        super.onTick(b, event, timeSinceStart);

        tally.add(new Notch(b, event, "tick"));
    }

    @Override
    public void onSuccess(ForkJoin b, ApplicationEvent event, Object target) {
        super.onSuccess(b, event, target);

        tally.add(new Notch(b, event, target));
    }

    @Override
    public void onException(ForkJoin b, ApplicationEvent event, Object target, Exception e) {
        super.onException(b, event, target, e);

        tally.add(new Notch(b, event, target, e));
    }
}
