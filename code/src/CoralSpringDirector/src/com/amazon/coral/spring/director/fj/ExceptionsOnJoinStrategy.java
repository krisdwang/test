package com.amazon.coral.spring.director.fj;

import java.util.Collection;

/**
 * A ForkJoin uses this strategy to handle the situation in which some calls to the onApplicationEvent methods of the
 * target beans have thrown an exception.
 * <p>
 * Note that each onApplicationEvent call runs in its separate worker thread.
 * </p>
 * 
 * @author Ovidiu Gheorghies
 */
public interface ExceptionsOnJoinStrategy {

    void onExceptions(Collection<Exception> exceptions) throws Exception;

}
