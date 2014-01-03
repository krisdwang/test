package com.amazon.coral.spring.director.fj;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Logs a WARN message if the configuration has no events or no targets.
 * 
 * @author Ovidiu Gheorghies
 */
public class DubiousConfigWarnStrategy implements DubiousConfigStrategy {
    private static final Log log = LogFactory.getLog(ForkJoin.class);

    @Override
    public void onNoEvent(ForkJoin b) {
        log.warn(String.format("No events defined in ForkJoin %s", b.toString()));
    }

    @Override
    public void onNoTargets(ForkJoin b) {
        log.warn(String.format("No targets defined in ForkJoin %s", b.toString()));
    }

}
