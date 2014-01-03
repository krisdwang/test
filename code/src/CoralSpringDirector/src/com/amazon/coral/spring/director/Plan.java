package com.amazon.coral.spring.director;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Sequentially calls two sets of Callable-s on Spring SmartLifecycle start and stop, respectively.
 * 
 * @author Ovidiu Gheorghies
 */
public class Plan implements SmartLifecycle {
    private static final Log log = LogFactory.getLog(Plan.class);

    /**
     * This constant specifies the default SmartLifecycle phase a Plan operates in.
     * <p>
     * It is understood that its value is higher than those of regular beans (&gt; 10^6), so that the Plan operates by
     * default "late" during context initialization and "early" during context shutdown. However, users should not rely
     * on it having a particular value. If SpringLifecycle is used for their own beans, users should ensure that the
     * phases of their beans and those of the plans are set in a way consistent with their goals.
     */
    public static final int DEFAULT_PHASE = 1000001;

    private int phase = DEFAULT_PHASE;

    private boolean isRunning;
    private boolean autoStartup = true;
    private Collection<Callable<?>> startupCallables = Collections.emptySet();
    private Collection<Callable<?>> shutdownCallables = Collections.emptySet();

    /**
     * Sequentially calls the set of startup callables.
     */
    @Override
    public void start() {
        log.debug("Executing startup plan");

        isRunning = true;

        callAll(startupCallables);
    }

    /**
     * Sequentially calls the set of shutdown callables.
     */
    @Override
    public void stop(Runnable callback) {
        log.debug("Executing shutdown plan");

        callAll(shutdownCallables);

        isRunning = false;
        callback.run();
    }

    private void callAll(Collection<Callable<?>> callables) {
        for (Callable<?> callable : callables) {
            try {
                callable.call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Never called.
     */
    @Override
    public void stop() {
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    public void setPhase(int phase) {
        this.phase = phase;
    }

    @Override
    public int getPhase() {
        return phase;
    }

    public void setAutoStartup(boolean autoStartup) {
        this.autoStartup = autoStartup;
    }

    @Override
    public boolean isAutoStartup() {
        return autoStartup;
    }

    public Collection<Callable<?>> getStartup() {
        return startupCallables;
    }

    public Collection<Callable<?>> getShutdown() {
        return shutdownCallables;
    }

    public void setStartup(Collection<Callable<?>> callables) {
        this.startupCallables = callables;
    }

    public void setShutdown(Collection<Callable<?>> callables) {
        this.shutdownCallables = callables;
    }
}
