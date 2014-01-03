package com.amazon.messaging.utils.command;

/**
 * @author kaitchuc A class that handles incoming events.
 * @param <IncomingType>
 *            The type of the command that is coming in.
 * @param <State>
 *            Any additional state the processor needs in order to perform its
 *            job.
 */
public interface Processor<IncomingType, State, MetaType> {

    <X extends IncomingType> void process(X command, State state);

    MetaType getType();

}
