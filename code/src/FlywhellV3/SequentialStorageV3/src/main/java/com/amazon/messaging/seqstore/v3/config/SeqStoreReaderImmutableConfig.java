package com.amazon.messaging.seqstore.v3.config;

import com.amazon.messaging.seqstore.v3.InflightInfoFactory;

import net.jcip.annotations.Immutable;
import lombok.Data;
import lombok.ToString;

@Data
@ToString(includeFieldNames=true)
@Immutable
public class SeqStoreReaderImmutableConfig<InfoType> implements SeqStoreReaderConfigInterface<InfoType> {
    private final InflightInfoFactory<InfoType> inflightInfoFactory;
    private final long startingMaxMessageLifetime;
    private final int maxInflightTableMessageCountLimit;
}
