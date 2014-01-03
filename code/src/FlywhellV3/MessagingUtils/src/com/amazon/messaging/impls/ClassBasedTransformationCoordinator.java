package com.amazon.messaging.impls;

import com.amazon.messaging.interfaces.Tagger;
import com.amazon.messaging.utils.command.AbstractTransformationCoordinator;
import com.amazon.messaging.utils.command.DefaultTransformer;

public class ClassBasedTransformationCoordinator<One, Many, State> extends AbstractTransformationCoordinator<One, Many, State, String> {

    public ClassBasedTransformationCoordinator(DefaultTransformer<One, Many, State> onNoTransformer,
            Tagger<One, Many, String, State> tagger) {
        super(onNoTransformer, tagger);
    }

    public <X extends Many> void addType(ClassBasedTransformer<One, Many, X, State> trans) {
        transformers.put(trans.getType().getName(), trans);
    }

    @Override
    protected <X extends Many> String getRolloutType(X command, State state) {
        return command.getClass().getName();
    }

}
