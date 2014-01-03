package com.amazon.messaging.interfaces;

public interface Tagger<One, Many, TagType, ExternalState> {

    /**
     * Marks the transformed object to ensure that the transformation is
     * reversible. Calling this method ensures that the Coordinator is able to
     * convert from Many to One, tag the new object and then convert that One
     * back into it's previous type of Many.
     * 
     * @param canonicalName
     * @param transformed
     * @return
     */
    abstract One addTag(Many orig, One transformed, ExternalState state);

    abstract TagType getTag(One transformed, ExternalState state);
}
