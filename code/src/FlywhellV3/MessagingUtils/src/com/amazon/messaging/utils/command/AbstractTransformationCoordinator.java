package com.amazon.messaging.utils.command;

import java.util.HashMap;
import java.util.Map;

import com.amazon.messaging.interfaces.Tagger;

/**
 * @author kaitchuc This class acts like a transformer but handles any one of
 *         many types. Given one common incoming type it will look at then
 *         decide which of the Transformers registered with it should handle the
 *         transformation. Then it will delegate the transformation to that
 *         transformer and return the result. If the object happens to contain
 *         nested objects the transformer may call into the
 *         TransformationCoordinator to transform the nested object. @See
 *         Transformer.java You have the option of also registering a processor
 *         to handle the resulting type once it has been transformed. This is
 *         convenient because it allows compile time validation of type safety.
 * @param <One>
 *            The incoming type to be handled.
 * @param <Many>
 *            The common super class or interface of the resulting
 *            transformation.
 * @param <State>
 *            Any additional state the individual transformers may need.
 *            Example: Suppose you wish to transform various objects to and from
 *            byte arrays. and have a class called MetaData which is needed to
 *            process the objects. You would create a TransformationCoordinator
 *            with the parameters <byte[],Object,MetaData> They you would call
 *            addType with each of the Transformers you wish to handle. Then you
 *            can call the process method on any byte arrays. The coordinator
 *            will call getTransformType which will look at the byte array and
 *            determine the type. This could for example be implemented by
 *            looking up some magic bytes in the MetaData. The transformer will
 *            be called and its result will be handed off to the corresponding
 *            processor. A neat feature of this class is that the resulting
 *            object is Strongly typed so your processor does not need to know
 *            what type the object was before the transformation, nor does it
 *            have to cast the object passed to it.
 */
public abstract class AbstractTransformationCoordinator<One, Many, State, MetaType> implements TransformationCoordinator<One, Many, State> {

    protected final Map<MetaType, Transformer<One, Many, ? extends Many, State>> transformers = new HashMap<MetaType, Transformer<One, Many, ? extends Many, State>>();

    private final DefaultTransformer<One, Many, State> onNoTransformer;

    private final Tagger<One, Many, MetaType, State> tagger;

    public AbstractTransformationCoordinator(DefaultTransformer<One, Many, State> onNoTransformer,
            Tagger<One, Many, MetaType, State> tagger) {
        this.onNoTransformer = onNoTransformer;
        this.tagger = tagger;
    }

    protected <X extends Many> void addType(MetaType type, Transformer<One, Many, X, State> trans) {
        transformers.put(type, trans);
    }

    @Override
    public <X extends Many> One rollout(X in, TransformationCoordinator<One, Many, State> parent, State state) {
        Transformer<One, Many, ? extends Many, State> transformer = null;
        MetaType type = getRolloutType(in, state);
        if (type != null)
            transformer = transformers.get(type);

        One transformed;
        if (transformer == null)
            transformed = onNoTransformer.defaultRollout(in, parent, state);
        else
            transformed = callRollout(in, parent, state, transformer);
        return tagger.addTag(in, transformed, state);
    }

    @SuppressWarnings("unchecked")
    private <X extends Many> One callRollout(Many in, TransformationCoordinator<One, Many, State> parent,
            State state, Transformer<One, Many, X, State> transformer) {
        return transformer.rollout((X) in, (parent == null) ? this : parent, state);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <X extends Many> X transform(One in, TransformationCoordinator<One, Many, State> parent,
            State state) {
        MetaType type = tagger.getTag(in, state);
        if (type == null)
            return onNoTransformer.<X> defaultTransform(in, parent, state);
        Transformer<One, Many, X, State> transformer = (Transformer<One, Many, X, State>) transformers.get(type);
        return callTransform(in, parent, state, transformer);
    }

    private <X extends Many> X callTransform(One in, TransformationCoordinator<One, Many, State> parent,
            State state, Transformer<One, Many, X, State> transformer) {
        if (transformer == null)
            return onNoTransformer.<X> defaultTransform(in, parent, state);

        return transformer.transform(in, (parent == null) ? this : parent, state);
    }

    protected abstract <X extends Many> MetaType getRolloutType(X command, State state);

}
