package com.amazon.coral.spring.director.test.sample;

import java.util.Collection;

import com.amazon.coral.spring.director.fj.ExceptionsOnJoinStrategy;
import com.amazon.coral.tally.Tally;

public class ExceptionsOnJoinCustomStrategy implements ExceptionsOnJoinStrategy {
    private final Tally tally;

    public ExceptionsOnJoinCustomStrategy(Tally eventLog) {
        this.tally = eventLog;
    }

    @Override
    public void onExceptions(Collection<Exception> exceptions) throws Exception {
        tally.add("exception");
    }
}
