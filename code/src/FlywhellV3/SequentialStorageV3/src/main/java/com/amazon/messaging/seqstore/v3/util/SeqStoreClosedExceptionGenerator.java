package com.amazon.messaging.seqstore.v3.util;

import com.amazon.messaging.seqstore.v3.exceptions.SeqStoreClosedException;
import com.amazon.messaging.utils.ClosableStateManager.CloseableExceptionGenerator;
import com.amazon.messaging.utils.ClosableStateManager.State;


public class SeqStoreClosedExceptionGenerator implements CloseableExceptionGenerator<SeqStoreClosedException> {
    private final String componentName;
    
    public SeqStoreClosedExceptionGenerator(String componentName) {
        super();
        this.componentName = componentName;
    }

    @Override
    public SeqStoreClosedException getException(String operation, State state) {
        return new SeqStoreClosedException("Cannot " + operation + " on closed " + componentName );
    }

}
