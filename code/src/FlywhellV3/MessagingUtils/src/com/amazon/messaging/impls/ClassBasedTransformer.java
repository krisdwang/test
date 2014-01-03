package com.amazon.messaging.impls;

import com.amazon.messaging.utils.command.Transformer;

public interface ClassBasedTransformer<Car, SuperType, Robot extends SuperType, ExternalState> extends Transformer<Car, SuperType, Robot, ExternalState> {

    Class<Robot> getType();
}
