package com.amazon.messaging.impls;

import com.amazon.messaging.utils.command.Processor;

/**
 * @author kaitchuc A class that handles incoming events.
 * @param <IncomingType>
 *            The type of the command that is coming in.
 * @param <State>
 *            Any additional state the processor needs in order to perform its
 *            job.
 */
public interface ClassBasedProcessor<IncomingType, State> extends Processor<IncomingType, State, Class<?>> {

    @Override
    <X extends IncomingType> void process(X command, State state);

    Class<IncomingType> getType();

}
