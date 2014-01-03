package com.amazon.coral.metrics.helper;

@SuppressWarnings("serial")
class QuerylogHelperConfigurationException extends RuntimeException {

  QuerylogHelperConfigurationException(String message) {
    super(message);
  }

  QuerylogHelperConfigurationException(Throwable cause) {
    super(cause);
  }

}
