package com.amazon.coral.spring.director.live;

import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.Callable;

import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;

import com.amazon.coral.spring.director.Plan;
import com.amazon.coral.spring.director.event.NamedEventDispatcher;
import com.amazon.coral.spring.director.fj.DubiousConfigStrategy;
import com.amazon.coral.spring.director.fj.DubiousConfigThrowStrategy;
import com.amazon.coral.spring.director.fj.ExceptionsOnJoinRethrowFirstStrategy;
import com.amazon.coral.spring.director.fj.ExceptionsOnJoinStrategy;
import com.amazon.coral.spring.director.fj.ForkJoin;
import com.amazon.coral.spring.director.fj.ForkJoinChain;
import com.amazon.coral.spring.director.fj.Listener;
import com.amazon.coral.spring.director.fj.ListenerLogger;

/**
 * Offers a shortcut for the more verbose configuration below.
 * 
 * <pre>
 *  {@code
 *  <bean id="planA" class="com.amazon.coral.spring.director.Plan">
 *        <property name="startup">
 *            <list>
 *                <bean class="com.amazon.coral.spring.director.ForkJoinChain">
 *                    <property name="events" value="acquire,activate" />
 *                    <property name="targets" value="#{{@workerA,@workerB,@workerC}}"/>
 *                </bean>
 *            </list>
 *        </property>
 *        <property name="shutdown">
 *            <list>
 *                <bean class="com.amazon.coral.spring.director.ForkJoinChain">
 *                    <property name="events" value="deactivate,release" />
 *                    <property name="targets" value="#{{@workerA,@workerB,@workerC}}"/>
 *                </bean>
 *            </list>
 *        </property>
 * </bean>
 *   }
 * </pre>
 * <p>
 * The equivalent configuration is:
 * </p>
 * 
 * <pre>
 * {@code
 * <bean id="planA" class="com.amazon.coral.spring.director.live.LivePlan">
 *     <property name="targets" value="#{{@workerA,@workerB,@workerC}}"/>
 * </bean>
 *  }
 * </pre>
 * <p>
 * The properties of the startup and shutdown ForkJoinChain-s can be set by prefixing their names with startup and
 * shutdown, respectively. For example:
 * </p>
 * 
 * <pre>
 * {@code
 * <bean id="planA" class="com.amazon.coral.spring.director.live.LivePlan">
 *       <property name="targets" value="#{{@workerA,@workerB,@workerC}}"/>
 *       
 *       <property name="startupListener" ref="myListener"/>
 *       <property name="shutdownTimeout" value="150000"/>
 * </bean>
 * }
 * </pre>
 * <p>
 * For extra brevity, if the target beans are POJO-s (for example, not derived from or adapted to LiveAdapter), they can
 * be automatically wrapped into a {@link NamedEventDispatcher} by using "simpleTargets" (or "simpleTarget"):
 * 
 * <pre>
 * {@code
 * <bean id="planA" class="com.amazon.coral.spring.director.live.LivePlan">
 *       <property name="plainTarget" value="plainOldBean"/>
 * </bean>
 * }
 * </pre>
 * 
 * </p>
 * 
 * @author Ovidiu Gheorghies
 */
public class LivePlan extends Plan {
    private Collection<ApplicationListener<? extends ApplicationEvent>> targets;

    private Listener startupListener = ListenerLogger.INSTANCE;
    private DubiousConfigStrategy startupDubiousConfigStrategy =
            DubiousConfigThrowStrategy.INSTANCE;
    private ExceptionsOnJoinStrategy startupExceptionsOnJoinStrategy =
            ExceptionsOnJoinRethrowFirstStrategy.INSTANCE;
    private long startupTickMillis = ForkJoin.DEFAULT_TICK;
    private long startupJoinTimeoutMillis = ForkJoin.DEFAULT_TIMEOUT;

    private Listener shutdownListener = ListenerLogger.INSTANCE;
    private DubiousConfigStrategy shutdownDubiousConfigStrategy =
            DubiousConfigThrowStrategy.INSTANCE;
    private ExceptionsOnJoinStrategy shutdownExceptionsOnJoinStrategy =
            ExceptionsOnJoinRethrowFirstStrategy.INSTANCE;
    private long shutdownTickMillis = ForkJoin.DEFAULT_TICK;
    private long shutdownJoinTimeoutMillis = ForkJoin.DEFAULT_TIMEOUT;

    public LivePlan() {
    }

    /**
     * Sets the targets by automatically wrapping input plain objects in {@link NamedEventDispatcher}-s.
     * 
     * @param targets
     */
    public void setPlainTargets(Collection<Object> targets) {
        this.targets = new LinkedList<ApplicationListener<? extends ApplicationEvent>>();
        for (Object target : targets) {
            this.targets.add(new NamedEventDispatcher(target));
        }
    }

    /**
     * Sets the target by automatically wrapping input plain objects it in a {@link NamedEventDispatcher}.
     * 
     * @param target
     */
    public void setPlainTarget(Object target) {
        this.targets = new LinkedList<ApplicationListener<? extends ApplicationEvent>>();
        this.targets.add(new NamedEventDispatcher(target));
    }

    public void setTargets(Collection<ApplicationListener<? extends ApplicationEvent>> targets) {
        this.targets = targets;
    }

    public void setTarget(ApplicationListener<? extends ApplicationEvent> target) {
        this.targets = new LinkedList<ApplicationListener<? extends ApplicationEvent>>();
        this.targets.add(target);
    }

    @Override
    public void start() {
        initializePlan();

        super.start();
    }

    @Override
    public void stop(Runnable callback) {
        super.stop(callback);
    }

    private void initializePlan() {
        if (super.getStartup().isEmpty()) {
            setStartup(buildForkJoinChainCallables(targets,
                    new String[] {LiveEvent.acquire.toString(), LiveEvent.activate.toString()},
                    startupListener,
                    startupDubiousConfigStrategy,
                    startupExceptionsOnJoinStrategy,
                    startupTickMillis,
                    startupJoinTimeoutMillis));
        }

        if (super.getShutdown().isEmpty()) {
            setShutdown(buildForkJoinChainCallables(targets,
                    new String[] {LiveEvent.deactivate.toString(), LiveEvent.release.toString()},
                    shutdownListener,
                    shutdownDubiousConfigStrategy,
                    shutdownExceptionsOnJoinStrategy,
                    shutdownTickMillis,
                    shutdownJoinTimeoutMillis));
        }
    }

    private LinkedList<Callable<?>> buildForkJoinChainCallables(
            Collection<ApplicationListener<? extends ApplicationEvent>> targets,
            String[] eventNames,
            Listener listener,
            DubiousConfigStrategy dubiousConfigStrategy,
            ExceptionsOnJoinStrategy exceptionsOnJoinStrategy,
            long tickMillis,
            long joinTimeoutMillis) {
        ForkJoinChain chain = new ForkJoinChain();
        chain.setTargets(targets);
        chain.setEvents(eventNames);
        chain.setListener(listener);
        chain.setDubiousConfigStrategy(dubiousConfigStrategy);
        chain.setExceptionsOnJoinStrategy(exceptionsOnJoinStrategy);
        chain.setTick(tickMillis);
        chain.setTimeout(joinTimeoutMillis);

        LinkedList<Callable<?>> callables = new LinkedList<Callable<?>>();
        callables.add(chain);

        return callables;
    }

    public DubiousConfigStrategy getStartupDubiousConfigStrategy() {
        return startupDubiousConfigStrategy;
    }

    public void setStartupDubiousConfigStrategy(DubiousConfigStrategy startupDubiousConfigStrategy) {
        this.startupDubiousConfigStrategy = startupDubiousConfigStrategy;
    }

    public ExceptionsOnJoinStrategy getStartupExceptionsOnJoinStrategy() {
        return startupExceptionsOnJoinStrategy;
    }

    public void setStartupExceptionsOnJoinStrategy(ExceptionsOnJoinStrategy startupExceptionsOnJoinStrategy) {
        this.startupExceptionsOnJoinStrategy = startupExceptionsOnJoinStrategy;
    }

    public long getStartupTick() {
        return startupTickMillis;
    }

    public void setStartupTick(long startupTickMillis) {
        this.startupTickMillis = startupTickMillis;
    }

    public long getStartupTimeout() {
        return startupJoinTimeoutMillis;
    }

    public void setStartupTimeout(long startupJoinTimeoutMillis) {
        this.startupJoinTimeoutMillis = startupJoinTimeoutMillis;
    }

    public DubiousConfigStrategy getShutdownDubiousConfigStrategy() {
        return shutdownDubiousConfigStrategy;
    }

    public void setShutdownDubiousConfigStrategy(DubiousConfigStrategy shutdownDubiousConfigStrategy) {
        this.shutdownDubiousConfigStrategy = shutdownDubiousConfigStrategy;
    }

    public ExceptionsOnJoinStrategy getShutdownExceptionsOnJoinStrategy() {
        return shutdownExceptionsOnJoinStrategy;
    }

    public void setShutdownExceptionsOnJoinStrategy(ExceptionsOnJoinStrategy shutdownExceptionsOnJoinStrategy) {
        this.shutdownExceptionsOnJoinStrategy = shutdownExceptionsOnJoinStrategy;
    }

    public long getShutdownTick() {
        return shutdownTickMillis;
    }

    public void setShutdownTick(long shutdownTickMillis) {
        this.shutdownTickMillis = shutdownTickMillis;
    }

    public long getShutdownTimeout() {
        return shutdownJoinTimeoutMillis;
    }

    public void setShutdownTimeout(long shutdownJoinTimeoutMillis) {
        this.shutdownJoinTimeoutMillis = shutdownJoinTimeoutMillis;
    }

    public Collection<ApplicationListener<? extends ApplicationEvent>> getTargets() {
        return targets;
    }

    public Listener getStartupListener() {
        return startupListener;
    }

    public void setStartupListener(Listener startupListener) {
        this.startupListener = startupListener;
    }

    public Listener getShutdownListener() {
        return shutdownListener;
    }

    public void setShutdownListener(Listener shutdownListener) {
        this.shutdownListener = shutdownListener;
    }
}
