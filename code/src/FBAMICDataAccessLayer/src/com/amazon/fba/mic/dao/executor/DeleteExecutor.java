package com.amazon.fba.mic.dao.executor;

/**
 * 
 * @author wdong
 *
 * @param <T>
 */
public interface DeleteExecutor<T> {

    public void delete(T persistentObject);
       
}
