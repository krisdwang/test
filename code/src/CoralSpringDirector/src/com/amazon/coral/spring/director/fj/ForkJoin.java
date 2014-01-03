package com.amazon.coral.spring.director.fj;

import java.util.Collection;
import java.util.Collections;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import com.amazon.coral.spring.director.event.NamedEvent;

/**
 * Forks one thread for each target bean, dispatches a given event to each bean in its corresponding thread, and then
 * joins the threads (blocks until all targets exit).
 * <p>
 * Each target bean handles the event in its own worker thread, provided by this ForkJoin. Dispatching completes
 * successfully when all worker threads complete without throwing an exception.
 * </p>
 * <p>
 * Any exception thrown by a target is recorded and subject to processing by the {@link ExceptionsOnJoinStrategy} upon
 * worker threads join. By default, this means that the first recorded exception is re-thrown.
 * </p>
 * 
 * @author Ovidiu Gheorghies
 */
public class ForkJoin implements Callable<Void> {
    public static final long DEFAULT_TICK = 1000;
    public static final long DEFAULT_TIMEOUT = 10 * 60 * 1000; // ten minutes

    private static final Log log = LogFactory.getLog(ForkJoin.class);

    private static final Timer timer = new Timer(true);
    private static final ThreadFactory threadFactory = new CustomizableThreadFactory("b.worker-");

    private ExecutorService executor;
    private Collection<ApplicationListener<? extends ApplicationEvent>> targets = Collections.emptyList();

    private Listener listener = ListenerLogger.INSTANCE;

    private long tick = DEFAULT_TICK;

    private long joinTimeoutMillis = DEFAULT_TIMEOUT;

    private ApplicationEvent event;

    private final Collection<Exception> exceptions = new ConcurrentLinkedQueue<Exception>();

    private ExceptionsOnJoinStrategy exceptionsOnJoinStrategy =
            ExceptionsOnJoinRethrowFirstStrategy.INSTANCE;
    private DubiousConfigStrategy dubiousConfigStrategy = DubiousConfigThrowStrategy.INSTANCE;

    public ForkJoin() {
    }

    public ForkJoin(ApplicationEvent event,
            Collection<ApplicationListener<? extends ApplicationEvent>> targets) {
        this.targets = targets;
        this.event = event;
    }

    @Override
    public Void call() throws Exception {
        listener.onBegin(this, event);

        if (event == null) {
            dubiousConfigStrategy.onNoEvent(this);
            return null;
        }

        if (targets.isEmpty()) {
            dubiousConfigStrategy.onNoTargets(this);
            return null;
        }

        TimerTask ticker = new TimedTimerTask() {
            @Override
            public void run() {
                listener.onTick(ForkJoin.this, event, timeSinceStart());
            }
        };

        try {
            executor = Executors.newFixedThreadPool(targets.size(), threadFactory);

            timer.scheduleAtFixedRate(ticker, tick, tick);

            for (ApplicationListener<? extends ApplicationEvent> target : targets) {
                executor.execute(new EventDispatcherRunnable(this, target, event));
            }

            executor.shutdown();
            boolean completedBeforeTimeout = executor.awaitTermination(joinTimeoutMillis, TimeUnit.MILLISECONDS);

            if (!exceptions.isEmpty()) {
                exceptionsOnJoinStrategy.onExceptions(exceptions);
            }

            if (!completedBeforeTimeout) {
                throw new TimeoutException(
                        String.format("Timeout of %d ms exceeded in %s", joinTimeoutMillis, this.toString()));
            }

            listener.onEnd(this, event);
        } finally {
            ticker.cancel();
        }

        return null;
    }

    protected <E extends ApplicationEvent> void onSuccess(E event, ApplicationListener<E> target) {
        listener.onSuccess(this, event, target);
    }

    protected <E extends ApplicationEvent> void onException(E event, ApplicationListener<E> target, Exception e) {
        listener.onException(this, event, target, e);

        this.exceptions.add(e);
    }

    public Collection<ApplicationListener<? extends ApplicationEvent>> getTargets() {
        return targets;
    }

    public void setTargets(Collection<ApplicationListener<? extends ApplicationEvent>> targets) {
        this.targets = targets;
    }

    public Listener getListener() {
        return listener;
    }

    public void setListener(Listener listener) {
        this.listener = listener;
    }

    public long getTick() {
        return tick;
    }

    public void setTick(long tickMillis) {
        this.tick = tickMillis;
    }

    public void setEvent(String event) {
        this.event = new NamedEvent(this, event);
    }

    public void setExceptionsOnJoinStrategy(ExceptionsOnJoinStrategy exceptionsOnJoinStrategy) {
        this.exceptionsOnJoinStrategy = exceptionsOnJoinStrategy;
    }

    public void setDubiousConfigStrategy(DubiousConfigStrategy strategy) {
        this.dubiousConfigStrategy = strategy;
    }

    public ExceptionsOnJoinStrategy getExceptionsOnJoinStrategy() {
        return exceptionsOnJoinStrategy;
    }

    public DubiousConfigStrategy getDubiousConfigStrategy() {
        return dubiousConfigStrategy;
    }

    /**
     * Sets the worker threads join timeout in milliseconds.
     * 
     * @param joinTimeoutMillis
     */
    public void setTimeout(long joinTimeoutMillis) {
        this.joinTimeoutMillis = joinTimeoutMillis;
    }

    public long getTimeout() {
        return joinTimeoutMillis;
    }

    @Override
    public String toString() {
        return String.format("ForkJoin@%08X", hashCode());
    }

    /**
     * A TimerTask that records its construction time and can compute the time passed since then.
     * 
     * @author Ovidiu Gheorghies
     */
    private abstract static class TimedTimerTask extends TimerTask {
        private final long ts;

        public TimedTimerTask() {
            this.ts = System.currentTimeMillis();
        }

        public long timeSinceStart() {
            return System.currentTimeMillis() - ts;
        }
    }

    /**
     * A Runnable that calls a Spring bean's onApplicationEvent and notifies the parent ForkJoin if the call was
     * successful or resulted in an exception.
     * 
     * @author Ovidiu Gheorghies
     */
    private static class EventDispatcherRunnable<E extends ApplicationEvent> implements Runnable {
        private final ApplicationListener<E> target;
        private final ForkJoin b;
        private final E event;

        public EventDispatcherRunnable(ForkJoin b, ApplicationListener<E> target, E event) {
            this.b = b;
            this.target = target;
            this.event = event;
        }

        @Override
        public void run() {
            try {
                target.onApplicationEvent(event);

                b.onSuccess(event, target);
            } catch (Exception e) {
                b.onException(event, target, e);
            }
        }
    }
}
