package com.amazon.fba.mic.dao.executor;

import java.lang.reflect.Method;


/**
 * 
 * @author wdong
 *
 */
public interface InternalDeleteExecutor {
	 public void internalDelete(Method method, Object args);
}
