package com.amazon.messaging.interfaces;

import java.lang.Number;

public interface MetricsSource {

    public abstract Number getMetricValue(String name);
}

