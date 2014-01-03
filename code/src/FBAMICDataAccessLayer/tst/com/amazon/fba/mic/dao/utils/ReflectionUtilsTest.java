package com.amazon.fba.mic.dao.utils;

import java.lang.reflect.Field;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.util.Assert;

public class ReflectionUtilsTest {

	TestBean bean = null;
	
	@Before
	public void setUp() throws Exception {
		bean = new TestBean();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetFieldValue() {
		boolean flag = (Boolean)ReflectionUtils.getFieldValue(bean, "flag");
		Assert.isTrue(flag == false);
	}

	@Test
	public void testSetFieldValue() {
		ReflectionUtils.setFieldValue(bean, "flag", true);
		boolean flag = (Boolean)ReflectionUtils.getFieldValue(bean, "flag");
		Assert.isTrue(flag);
	}

	@Test
	public void testGetDeclaredField() {
		Field f = ReflectionUtils.getDeclaredField(bean, "flag");
		f.getName().equals("flag");
	}

	@Test
	public void testMakeAccessible() {
		Field f = ReflectionUtils.getDeclaredField(bean, "flag");
		ReflectionUtils.makeAccessible(f);
		Assert.isTrue(f.isAccessible());
	}

	private class TestBean {
		private boolean flag = false;
	}
}
