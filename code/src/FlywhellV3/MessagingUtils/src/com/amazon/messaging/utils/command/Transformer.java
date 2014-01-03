package com.amazon.messaging.utils.command;

/**
 * @author kaitchuc A class that can convert from one type of object to another
 *         and back. This class may call into the TransformationCoordinator if
 *         needed. Its roll is to find the appropriate transformer for any
 *         nested objects. This way each transformer need only know about its
 *         own classes not what they may happen to have nested within them.
 * @param <Robot>
 *            The default type that you are transforming from.
 * @param <SuperType>
 *            The super type from which all of the types you are transforming to
 *            inherit. This should just be Robot's super class.
 * @param <Car>
 *            The type that you transforming into.
 * @param <ExternalState>
 *            Any additional information that the Transformer may need in order
 *            to do its Job.
 */
public interface Transformer<Car, SuperType, Robot extends SuperType, ExternalState> {

    public abstract Car rollout(Robot in,
            TransformationCoordinator<Car, SuperType, ExternalState> coordinator, ExternalState state);

    public abstract Robot transform(Car in,
            TransformationCoordinator<Car, SuperType, ExternalState> coordinator, ExternalState state);
}
