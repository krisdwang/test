/*********************************************************************************
 * <p>
 * Perforce File Stats:
 * 
 * <pre>
 * $Id: //brazil/src/appgroup/appgroup/libraries/FBAMICDataAccessLayer/mainline/src/com/amazon/fba/mic/dao/finder/impl/FinderIntroductionInterceptor.java#4 $
 * $DateTime: 2013/03/04 03:27:29 $
 * $Change: 7030691 $
 * </pre>
 * 
 * </p>
 * 
 * @author $Author: wdong $
 * @version $Revision: #4 $ Copyright Notice This file contains proprietary
 * information of Amazon.com. Copying or reproduction without prior written
 * approval is prohibited. Copyright (c) 2012 Amazon.com. All rights reserved.
 *********************************************************************************/
package com.amazon.fba.mic.dao.finder.impl;

import java.util.Arrays;
import java.util.Map;

import org.aopalliance.intercept.MethodInvocation;
import org.springframework.aop.support.DelegatingIntroductionInterceptor;

import com.amazon.fba.mic.dao.AbstractDomainObject;
import com.amazon.fba.mic.dao.executor.InternalCreateExecutor;
import com.amazon.fba.mic.dao.executor.InternalDeleteExecutor;
import com.amazon.fba.mic.dao.executor.InternalUpdateExecutor;
import com.amazon.fba.mic.dao.finder.FinderExecutor;
import com.amazon.fba.mic.dao.namingstrategy.impl.Operation;
import com.amazon.fba.mic.dao.utils.ParamMethod;

/**
 * Introduction advice
 * 
 * @author wdong
 * @param <T>
 */
public class FinderIntroductionInterceptor<T> extends DelegatingIntroductionInterceptor {

    private static final long serialVersionUID = 4780921524594889019L;
    
    private static String[] crudMethods = new String[] { Operation.RETRIEVE, Operation.CREATE, Operation.COUNT, Operation.QUERY, Operation.UPDATE, Operation.DELETE};

    @SuppressWarnings({ "unchecked" })
    public Object invoke(MethodInvocation methodInvocation) throws Throwable { 
    	
        String methodName = methodInvocation.getMethod().getName();

        /**
         * Following conversation better than configuration, we prefer SDEs to add prefix to their query methods, like find, list or get.
         * The related document will be published on wiki {@link https://w.amazon.com/index.php/FBA/MIC/DataAccessLayer}
         */
        if (methodName.startsWith("find") || methodName.startsWith("list") || methodName.startsWith("get") || methodName.startsWith("load")) {
        	
        	FinderExecutor<T> executor = (FinderExecutor<T>) methodInvocation.getThis();
            return executor.executeFinder(methodInvocation.getMethod(), methodInvocation.getArguments());
            
        } else if (methodName.startsWith(Operation.UPDATE) && !methodName.endsWith(Operation.UPDATE)) { //updateXXX
        	InternalUpdateExecutor updateExecutor = (InternalUpdateExecutor)methodInvocation.getThis();  
        	
            if (methodInvocation.getArguments().length == 1 && methodInvocation.getArguments()[0] instanceof AbstractDomainObject) {            	
                return updateExecutor.internalUpdate(methodInvocation.getMethod(), methodInvocation.getArguments()[0]);
            } else {            	
                return updateExecutor.internalUpdate(methodInvocation.getMethod(), new ParamMethod(methodInvocation.getMethod()).getParam(methodInvocation.getArguments()));
            }
            
        } else if (methodName.startsWith(Operation.DELETE) && !methodName.endsWith(Operation.DELETE)) {
        	InternalDeleteExecutor deleteExecutor = (InternalDeleteExecutor)methodInvocation.getThis(); 
        	if (methodInvocation.getArguments().length == 1 && methodInvocation.getArguments()[0] instanceof AbstractDomainObject) {
            	
                deleteExecutor.internalDelete(methodInvocation.getMethod(), methodInvocation.getArguments()[0]);
            } else {            	            	
                deleteExecutor.internalDelete(methodInvocation.getMethod(), new ParamMethod(methodInvocation.getMethod()).getParam(methodInvocation.getArguments()));
            }
        	
        }  else if (methodName.startsWith(Operation.CREATE) && !methodName.endsWith(Operation.CREATE)) {
        	InternalCreateExecutor createExecutor = (InternalCreateExecutor)methodInvocation.getThis();
        	if (methodInvocation.getArguments().length == 1 && methodInvocation.getArguments()[0] instanceof AbstractDomainObject) {            	
                return createExecutor.internalCreate(methodInvocation.getMethod(), methodInvocation.getArguments()[0]);
            } else {            	
                return createExecutor.internalCreate(methodInvocation.getMethod(), new ParamMethod(methodInvocation.getMethod()).getParam(methodInvocation.getArguments()));
            }
        }else if (Arrays.asList(crudMethods).contains(methodName)) {
            /**
             * get the override method with Map argument
             */
            if (methodInvocation.getArguments()[0] instanceof Map) {
                return methodInvocation.proceed();
            }
            
        } 
        
        return methodInvocation.proceed();
    }

}