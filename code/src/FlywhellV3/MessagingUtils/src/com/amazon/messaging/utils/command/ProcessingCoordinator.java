package com.amazon.messaging.utils.command;

import java.util.HashMap;
import java.util.Map;

public class ProcessingCoordinator<Many, State, MetaType> implements Processor<Many, State, MetaType> {

    private final Map<MetaType, Processor<? extends Many, State, MetaType>> processors = new HashMap<MetaType, Processor<? extends Many, State, MetaType>>();

    private final DefaultProcessor<Many, State> onNoProcessor;

    private final MetaType type;

    public ProcessingCoordinator(MetaType type, DefaultProcessor<Many, State> onNoProcessor) {
        this.type = type;
        this.onNoProcessor = onNoProcessor;
    }

    public <X extends Many> void addType(Processor<X, State, MetaType> processor) {
        processors.put(processor.getType(), processor);
    }

    @SuppressWarnings("unchecked")
    private <X extends Many> Processor<X, State, MetaType> getProcessor(Many processorType) {
        return (Processor<X, State, MetaType>) processors.get(processorType.getClass());
    }

    public <X extends Many> void process(X command, State state) {
        Processor<X, State, MetaType> processor = null;
        if (command != null) {
            processor = getProcessor(command);
        }
        if (processor == null)
            onNoProcessor.defaultProcess(command, state);
        else
            processor.process(command, state);
    }

    @Override
    public MetaType getType() {
        return type;
    }
}
