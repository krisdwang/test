package com.amazon.messaging.utils.command;

public interface TransformationCoordinator<One, Many, State> {

    public <X extends Many> One rollout(X in, TransformationCoordinator<One, Many, State> parent, State state);

    public <X extends Many> X transform(One in, TransformationCoordinator<One, Many, State> parent,
            State state);

}
