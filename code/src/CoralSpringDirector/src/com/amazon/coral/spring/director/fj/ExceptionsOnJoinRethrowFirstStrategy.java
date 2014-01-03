package com.amazon.coral.spring.director.fj;

import java.util.Collection;

/**
 * Rethrows the first gathered exception exception thrown by one of the ForkJoin worker threads. This is the default
 * implementation used by a ForkJoin.
 * <p>
 * The exception is thrown in Spring's main thread, resulting in immediate context shutdown.
 * </p>
 * 
 * @author Ovidiu Gheorghies
 */
public class ExceptionsOnJoinRethrowFirstStrategy implements ExceptionsOnJoinStrategy {
    public static final ExceptionsOnJoinRethrowFirstStrategy INSTANCE =
            new ExceptionsOnJoinRethrowFirstStrategy();

    @Override
    public void onExceptions(Collection<Exception> exceptions) throws Exception {
        throw exceptions.iterator().next();
    }
}
