package com.amazon.messaging.utils.command;

public interface DefaultProcessor<Type, State> {

    <X extends Type> void defaultProcess(X command, State state);

}
