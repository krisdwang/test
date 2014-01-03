package com.amazon.coral.spring.director.event;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.springframework.context.ApplicationListener;

/**
 * Dispatches through reflection a named event to the namesake method of the target object.
 * <p>
 * For example, if this bean receives a {@link NamedEvent} with payload "activate", the method "activate()" of the
 * target bean is called.
 * </p>
 * 
 * @author Ovidiu Gheorghies
 */
public class NamedEventDispatcher implements ApplicationListener<NamedEvent> {
    private Object target;

    private EventStrategy unknownEventStrategy = UnknownEventThrowStrategy.INSTANCE;

    public NamedEventDispatcher() {
    }

    public NamedEventDispatcher(Object target) {
        setTarget(target);
    }

    public void setTarget(Object target) {
        this.target = target;
    }

    public Object getTarget() {
        return target;
    }

    public void setUnknownEventStrategy(EventStrategy eventStrategy) {
        unknownEventStrategy = eventStrategy;
    }

    @Override
    public void onApplicationEvent(NamedEvent event) {
        String name = event.getPayload();

        try {
            Method method = target.getClass().getMethod(name);
            method.setAccessible(true);
            method.invoke(target);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e.getTargetException());
        } catch (Exception e) {
            unknownEventStrategy.onEvent(target, event);
        }
    }
}
