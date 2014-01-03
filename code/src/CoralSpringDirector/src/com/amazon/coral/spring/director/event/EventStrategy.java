package com.amazon.coral.spring.director.event;

import org.springframework.context.ApplicationEvent;

/**
 * Strategy to be used by event receivers in some particular conditions.
 * 
 * @author Ovidiu Gheorghies
 */
public interface EventStrategy {
    void onEvent(Object target, ApplicationEvent event);
};
