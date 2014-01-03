package com.amazon.fba.mic.dao.route;

import org.springframework.aop.support.DefaultIntroductionAdvisor;

import com.amazon.fba.mic.dao.impl.GenericDaoImpl;

public class DynaDSAnnotationAdvisor<T> extends DefaultIntroductionAdvisor {
	

	private static final long serialVersionUID = -2625315319893236936L;

	public DynaDSAnnotationAdvisor() {
		super(new DynaDSAnnotationIntercepter<T>());
	}
	
	@SuppressWarnings("rawtypes") 
	public boolean matches(Class clazz) {
        return GenericDaoImpl.class.isAssignableFrom(clazz);
    }
}
