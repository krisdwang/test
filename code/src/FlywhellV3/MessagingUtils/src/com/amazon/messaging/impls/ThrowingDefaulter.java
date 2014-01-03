package com.amazon.messaging.impls;

import com.amazon.messaging.utils.command.DefaultProcessor;
import com.amazon.messaging.utils.command.DefaultTransformer;
import com.amazon.messaging.utils.command.TransformationCoordinator;

public class ThrowingDefaulter<One, Many, State> implements DefaultProcessor<Many, State>, DefaultTransformer<One, Many, State> {

    private final String context;

    public ThrowingDefaulter(String context) {
        this.context = context;
    }

    @Override
    public <X extends Many> void defaultProcess(X command, State state) {
        throw new IllegalArgumentException("Was provided with the command: " + command
                + " for which the class: " + ((command == null) ? null : command.getClass())
                + " was not registered with the TransformationCoordinator." + context);
    }

    @Override
    public <X extends Many> One defaultRollout(X in, TransformationCoordinator<One, Many, State> coordinator,
            State state) {
        throw new IllegalArgumentException("Was provided with the command: " + in
                + " whos type was not registered with the TransformationCoordinator." + context);
    }

    @Override
    public <X extends Many> X defaultTransform(One in,
            TransformationCoordinator<One, Many, State> coordinator, State state) {
        throw new IllegalArgumentException("Was provided with the command: " + in
                + " whos type was not registered with the TransformationCoordinator." + context);
    }
}
