package com.amazon.fba.mic.metrics;

import org.springframework.aop.support.DefaultIntroductionAdvisor;

import com.amazon.fba.mic.dao.impl.GenericDaoImpl;

public class ProfilableAnnotationAdvisor <T> extends DefaultIntroductionAdvisor {	

	private static final long serialVersionUID = -6188275455081865242L;

	public ProfilableAnnotationAdvisor(MetricsContext metricsContext) {
		super(new ProfilableAnnotationIntercepter<T>(metricsContext));
	}

	@SuppressWarnings("rawtypes") 
	public boolean matches(Class clazz) {
        //return GenericDaoImpl.class.isAssignableFrom(clazz);
		return true;
    }
}
