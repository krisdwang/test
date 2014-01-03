package com.amazon.fba.mic.dao;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.Before;
import org.junit.Test;
import org.springframework.util.Assert;

/**
 * 
 * @author wdong
 * 
 */
public class AbstractDomainObjectTest {

	private AbstractDomainObject<AbstractPrimaryKey> domainObject;

	@Before
	public void setUp() {
		domainObject = new AbstractDomainObject<AbstractPrimaryKey>() {
			// nothing
		};		
	}

	@Test
	public void testGetPersisted() {
		Assert.isTrue(domainObject.isPersisted() == false);
	}

	@Test
	public void testSetPersisted() {
		domainObject.setPersisted(true);
		Assert.isTrue(domainObject.isPersisted() == true);
	}
	
	@Test
	public void testGetRecordVersion(){
		Assert.isNull(domainObject.getRecordVersion());
	}
	
	@Test
	public void testSetRecordVersion() {
		domainObject.setRecordVersion(1L);
		Assert.notNull(domainObject.getRecordVersion());
		Assert.isTrue(domainObject.getRecordVersion() == 1L);
	}
	
	@Test
	public void testAnnotation() {
		Annotation[] annotations = TestAnnotation.class.getAnnotations();
		for(Annotation annotation : annotations) {
			if(annotation instanceof TT) {
				System.out.println("aaa");
			}
		}
	}
	
	@TT
	private class TestAnnotation {
		
	}
	
	@Inherited
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	private @interface TT {
		
	}
}
