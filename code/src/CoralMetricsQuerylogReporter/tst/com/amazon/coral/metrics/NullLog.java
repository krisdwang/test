package com.amazon.coral.metrics;

import org.apache.commons.logging.Log;

public class NullLog implements Log {

  public boolean isTraceEnabled() { return false; }
  public void trace(Object msg){}
  public void trace(Object msg, Throwable t){}

  public boolean isDebugEnabled() { return false; }
  public void debug(Object msg){}
  public void debug(Object msg, Throwable t){}

  public boolean isInfoEnabled(){ return false; }
  public void info(Object msg){}
  public void info(Object msg, Throwable t){}

  public boolean isWarnEnabled(){ return false; }
  public void warn(Object msg){}
  public void warn(Object msg, Throwable t){}

  public boolean isErrorEnabled(){ return false; }
  public void error(Object msg){}
  public void error(Object msg, Throwable t){}

  public boolean isFatalEnabled(){ return false; }
  public void fatal(Object msg){}
  public void fatal(Object msg, Throwable t){}
}
