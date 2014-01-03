package com.amazon.messaging.impls;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.messaging.utils.command.DefaultProcessor;
import com.amazon.messaging.utils.command.DefaultTransformer;
import com.amazon.messaging.utils.command.TransformationCoordinator;

public class LoggingDefaulter<One, Many, State> implements DefaultProcessor<Many, State>, DefaultTransformer<One, Many, State> {

    private static Log log = LogFactory.getLog(LoggingDefaulter.class);

    private final String context;

    public LoggingDefaulter(String context) {
        this.context = context;
    }

    @Override
    public <X extends Many> void defaultProcess(X command, State state) {
        log.warn("Was provided with the command: " + command + " for which the class: "
                + ((command == null) ? null : command.getClass())
                + " was not registered with the TransformationCoordinator." + context);
    }

    @Override
    public <X extends Many> One defaultRollout(X in, TransformationCoordinator<One, Many, State> coordinator,
            State state) {
        log.warn("Was provided with the command: " + in
                + " whos type was not registered with the TransformationCoordinator." + context);
        return null;
    }

    @Override
    public <X extends Many> X defaultTransform(One in,
            TransformationCoordinator<One, Many, State> coordinator, State state) {
        log.warn("Was provided with ths command: " + in
                + " whos type was not registered with the TransformationCoordinator." + context);
        return null;
    }

}
