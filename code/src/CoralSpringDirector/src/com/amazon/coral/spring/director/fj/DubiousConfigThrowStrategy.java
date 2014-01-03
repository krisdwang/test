package com.amazon.coral.spring.director.fj;

/**
 * Throws IllegalStateException if the configuration has no events or no targets. This is the default implementation
 * used by a ForkJoin.
 * 
 * @author Ovidiu Gheorghies
 */
public class DubiousConfigThrowStrategy implements DubiousConfigStrategy {
    public static final DubiousConfigThrowStrategy INSTANCE = new DubiousConfigThrowStrategy();

    @Override
    public void onNoEvent(ForkJoin b) {
        throw new IllegalStateException("No events defined in the ForkJoin. What is its purpose then?");
    }

    @Override
    public void onNoTargets(ForkJoin b) {
        throw new IllegalStateException("No targets defined in the ForkJoin. What is its purpose then?");
    }
}
