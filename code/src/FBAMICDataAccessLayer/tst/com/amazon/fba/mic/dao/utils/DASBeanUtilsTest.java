package com.amazon.fba.mic.dao.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.util.Assert;

import com.amazon.fba.mic.dao.exception.DataAccessDaoException;

public class DASBeanUtilsTest {
	
	

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testCopyProperties() {
		TestBean a = new TestBean();
		a.setCount(1);
		a.setName("test");
		TestBean b = new TestBean();
		DASBeanUtils.copyProperties(a, b);
		Assert.isTrue(a.getCount() == b.getCount());
		Assert.isTrue(a.getName().equals(b.getName()));
	}
	
	@Test
	public void testCopyPropertiesWithException() {
		TestBean a = new TestBean();
		a.setCount(1);
		a.setName("test");
		
		TestBean2 b = null;
		try{
			DASBeanUtils.copyProperties(a, b);
		} catch(Exception ex) {
			Assert.isTrue(ex instanceof DataAccessDaoException);
		}
	}
	
	private class TestBean{
		private int count;
		private String name;
		
		public int getCount() {
			return count;
		}
		public void setCount(int count) {
			this.count = count;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
	}

	private class TestBean2{
		private boolean flag;

		public boolean isFlag() {
			return flag;
		}

		public void setFlag(boolean flag) {
			this.flag = flag;
		}		
	}
}
