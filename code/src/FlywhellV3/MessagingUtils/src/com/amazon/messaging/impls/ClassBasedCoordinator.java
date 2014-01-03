package com.amazon.messaging.impls;

import com.amazon.messaging.interfaces.Tagger;
import com.amazon.messaging.utils.command.DefaultProcessor;
import com.amazon.messaging.utils.command.DefaultTransformer;
import com.amazon.messaging.utils.command.ProcessingCoordinator;
import com.amazon.messaging.utils.command.TransformationCoordinator;

/**
 * @author kaitchuc A less generalized, but more useful version of
 *         TransformationCoordinator. This class expects that the Processors and
 *         Transformers registered handle incoming events of a particular type
 *         which happen to be based on the Java Class of the incoming event.
 * @param <One>
 *            The incoming type to be handled.
 * @param <Many>
 *            The common super class or interface of the resulting
 *            transformation.
 * @param <State>
 *            Any additional state the individual transformers may need.
 */
public class ClassBasedCoordinator<One, Many, State> implements ClassBasedProcessor<One, State> {

    private final ClassBasedTransformationCoordinator<One, Many, State> transformer;

    private final ProcessingCoordinator<Many, State, Class<?>> processor;

    private final Class<One> inType;
    
    public ClassBasedCoordinator(Class<One> inType, Class<Many> outType,
            Tagger<One, Many, String, State> tagger, DefaultProcessor<Many, State> defaultProcessor,
            DefaultTransformer<One, Many, State> defaultTransformer) {
        super();
        this.inType = inType;
        transformer = new ClassBasedTransformationCoordinator<One, Many, State>(defaultTransformer, tagger);
        processor = new ProcessingCoordinator<Many, State, Class<?>>(outType, defaultProcessor);
    }

    public <X extends Many> void addType(ClassBasedTransformer<One, Many, X, State> trans) {
        transformer.addType(trans);
    }

    public <X extends Many> void addType(ClassBasedTransformer<One, Many, X, State> trans,
                                         ClassBasedProcessor<X, State> cp) {
        processor.addType(cp);
        transformer.addType(trans);
    }

    public <X extends Many> void addType(ClassBasedProcessor<X, State> cp) {
        processor.addType(cp);
    }

    private <X extends Many> X transform(One in, TransformationCoordinator<One, Many, State> parent,
            State state) {
        return transformer.<X> transform(in, parent, state);
    }

    @Override
    public <X extends One> void process(X command, State state) {
        processor.process(this.<Many> transform(command, null, state), state);
    }

    @Override
    public Class<One> getType() {
        return inType;
    }

}
