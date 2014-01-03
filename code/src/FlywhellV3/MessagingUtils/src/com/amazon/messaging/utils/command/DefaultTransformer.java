package com.amazon.messaging.utils.command;

public interface DefaultTransformer<Car, Robot, ExternalState> {

    public abstract <X extends Robot> Car defaultRollout(X in,
            TransformationCoordinator<Car, Robot, ExternalState> coordinator, ExternalState state);

    public abstract <X extends Robot> X defaultTransform(Car in,
            TransformationCoordinator<Car, Robot, ExternalState> coordinator, ExternalState state);

}
