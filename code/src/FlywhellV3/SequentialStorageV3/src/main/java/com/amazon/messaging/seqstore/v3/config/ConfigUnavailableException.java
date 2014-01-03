package com.amazon.messaging.seqstore.v3.config;

/**
 * Thrown by the config provider whenever the config is unavailable. 
 * 
 * @author stevenso
 *
 */
public class ConfigUnavailableException extends Exception {
    private static final long serialVersionUID = 1L;

    public ConfigUnavailableException() {
        super();
    }

    public ConfigUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConfigUnavailableException(String message) {
        super(message);
    }

    public ConfigUnavailableException(Throwable cause) {
        super(cause);
    }
}
