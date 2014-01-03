package com.amazon.coral.spring.director.fj;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;

import com.amazon.coral.spring.director.event.NamedEvent;

/**
 * A shortcut for a chain of ForkJoin-s operating on the same targets.
 * <p>
 * In the example below, planA and planB are equivalent.
 * </p>
 * 
 * <pre>
 * {@code
 *   <bean id="planA" class="com.amazon.coral.spring.director.Plan">
 *       <property name="startup">
 *           <list>
 *               <bean class="com.amazon.coral.spring.director.ForkJoinChain">
 *                   <property name="events" value="acquire,activate" />
 *                   <property name="targets" value="#{{@workerA,@workerB,@workerC}}"/>
 *               </bean>
 *           </list>
 *       </property>
 *   </bean>
 *   
 *   <bean id="planB" class="com.amazon.coral.spring.director.Plan">
 *       <property name="startup">
 *           <list>
 *               <bean class="com.amazon.coral.spring.director.ForkJoin">
 *                   <property name="event" value="acquire" />
 *                   <property name="targets" value="#{{@workerA,@workerB,@workerC}}"/>
 *               </bean>
 *               <bean class="com.amazon.coral.spring.director.ForkJoin">
 *                   <property name="event" value="activate" /> 
 *                   <property name="targets" value="#{{@workerA,@workerB,@workerC}}"/>
 *               </bean> 
 *           </list> 
 *       </property> 
 *  </bean> 
 *  }
 * </pre>
 * <p>
 * Properties set for the ForkJoinChain are forwarded to the internally created {@link ForkJoin}-s.
 * </p>
 * 
 * @author Ovidiu Gheorghies
 */
public class ForkJoinChain implements Callable<Void> {
    private static final Log log = LogFactory.getLog(ForkJoinChain.class);

    private final Collection<ApplicationEvent> events = new LinkedList<ApplicationEvent>();

    private long tick = ForkJoin.DEFAULT_TICK;
    private long joinTimeoutMillis = ForkJoin.DEFAULT_TIMEOUT;

    private final Collection<ApplicationListener<? extends ApplicationEvent>> targets =
            new LinkedList<ApplicationListener<? extends ApplicationEvent>>();

    private Listener listener = ListenerLogger.INSTANCE;

    private ExceptionsOnJoinStrategy exceptionsOnJoinStrategy =
            ExceptionsOnJoinRethrowFirstStrategy.INSTANCE;

    private DubiousConfigStrategy dubiousConfigStrategy =
            DubiousConfigThrowStrategy.INSTANCE;

    private final ArrayList<ForkJoin> forkJoins = new ArrayList<ForkJoin>();

    public void setEvents(String[] eventNames) {
        for (String eventName : eventNames) {
            events.add(new NamedEvent(this, eventName));
        }
    }

    /**
     * Sets the named events as comma-separated values from the string.
     * <p>
     * Each event will be dispatched by an internally created {@link ForkJoin} to the specified targets. The next event
     * is dispatched after the previous event is dispatched successfully by its corresponding ForkJoin.
     * </p>
     * 
     * @param eventNames
     */
    public void setEvents(String eventNames) {
        for (String eventName : eventNames.split(",")) {
            events.add(new NamedEvent(this, eventName));
        }
    }

    /**
     * These targets will be forwarded to the internally created {@link ForkJoin}-s.
     * 
     * @param targets
     */
    public void setTargets(Collection<ApplicationListener<? extends ApplicationEvent>> targets) {
        this.targets.addAll(targets);
    }

    public Listener getListener() {
        return listener;
    }

    /**
     * This listener will be forwarded to the internally created {@link ForkJoin}-s.
     * 
     * @param listener
     */
    public void setListener(Listener listener) {
        this.listener = listener;
    }

    /**
     * This configuration will be forwarded to the internally created {@link ForkJoin}-s.
     * 
     * @param tickMillis
     */
    public void setTick(long tickMillis) {
        this.tick = tickMillis;
    }

    /**
     * Gets the internally created forkJoins. Useful for debugging.
     * 
     * @return The internally created forkJoins.
     */
    public ForkJoin[] getForkJoinsAsArray() {
        return forkJoins.toArray(new ForkJoin[forkJoins.size()]);
    }

    @Override
    public Void call() throws Exception {
        for (ApplicationEvent event : events) {
            ForkJoin b = new ForkJoin(event, targets);
            b.setListener(listener);
            b.setExceptionsOnJoinStrategy(exceptionsOnJoinStrategy);
            b.setDubiousConfigStrategy(dubiousConfigStrategy);
            b.setTimeout(joinTimeoutMillis);
            b.setTick(tick);

            forkJoins.add(b);

            b.call();
        }
        return null;
    }

    public long getTick() {
        return tick;
    }

    public Collection<ApplicationListener<? extends ApplicationEvent>> getTargets() {
        return targets;
    }

    public ExceptionsOnJoinStrategy getExceptionsOnJoinStrategy() {
        return exceptionsOnJoinStrategy;
    }

    /**
     * This strategy will be forwarded to the internally created {@link ForkJoin}-s.
     * 
     * @param exceptionsOnJoinStrategy
     */
    public void setExceptionsOnJoinStrategy(ExceptionsOnJoinStrategy exceptionsOnJoinStrategy) {
        this.exceptionsOnJoinStrategy = exceptionsOnJoinStrategy;
    }

    /**
     * This strategy will be forwarded to the internally created {@link ForkJoin}-s.
     * 
     * @param strategy
     */
    public void setDubiousConfigStrategy(DubiousConfigStrategy strategy) {
        this.dubiousConfigStrategy = strategy;
    }

    public DubiousConfigStrategy getDubiousConfigStrategy() {
        return dubiousConfigStrategy;
    }

    /**
     * This configuration will be forwarded to the internally created {@link ForkJoin}-s.
     * 
     * @param joinTimeoutMillis
     */
    public void setTimeout(long joinTimeoutMillis) {
        this.joinTimeoutMillis = joinTimeoutMillis;
    }
}
