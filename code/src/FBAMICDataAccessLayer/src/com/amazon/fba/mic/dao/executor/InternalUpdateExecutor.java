package com.amazon.fba.mic.dao.executor;

import java.lang.reflect.Method;

/**
 * 
 * @author wdong
 *
 */
public interface InternalUpdateExecutor {
    public int internalUpdate(Method method, Object args);
}
