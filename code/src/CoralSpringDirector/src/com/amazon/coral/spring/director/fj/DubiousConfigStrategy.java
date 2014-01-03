package com.amazon.coral.spring.director.fj;

/**
 * A ForkJoin uses this strategy to handle a configuration with no events or no targets.
 * 
 * @author Ovidiu Gheorghies
 */
public interface DubiousConfigStrategy {
    void onNoEvent(ForkJoin b);

    void onNoTargets(ForkJoin b);
}
