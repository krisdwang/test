package com.amazon.fba.mic.dao.namingstrategy.impl;

import java.lang.reflect.Method;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.amazon.fba.mic.dao.namingstrategy.NamingStrategy;

public class NamingStrategyImplTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testQueryNameFromMethod() throws SecurityException, NoSuchMethodException {
		Method method = TestBean.class.getMethod("testMethod");
		NamingStrategy s = new NamingStrategyImpl();
		String qualifyName = s.queryNameFromMethod(TestBean.class, method);
		Assert.assertEquals(qualifyName, "com.amazon.fba.mic.dao.namingstrategy.impl.NamingStrategyImplTest$TestBean.testMethod");
	}

	private class TestBean {		
		public void testMethod(){
			//do nothing
		}
	}
}
