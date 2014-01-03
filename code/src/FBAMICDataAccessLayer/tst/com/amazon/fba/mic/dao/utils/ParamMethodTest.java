package com.amazon.fba.mic.dao.utils;

import java.lang.reflect.Method;
import java.util.List;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.binding.BindingException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.util.Assert;

public class ParamMethodTest {

	ParamMethod pm;
	Class<?>[] clazz = {String.class};
	Method method = null;
	
	@Before
	public void setUp() throws Exception {
		 method = ITestBean.class.getMethod("getStringList",clazz);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testParamMethod() throws SecurityException, NoSuchMethodException {
		
		pm = new ParamMethod(method);
		Assert.notNull(pm);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testGetParam() {		
		pm = new ParamMethod(method);
		Object params = pm.getParam(new String[] {"fba"});
		Assert.notNull(params);
		ParamMethod.MapperParamMap<String> paramMap = (ParamMethod.MapperParamMap<String>)params;
		String value = paramMap.get("name");
		Assert.isTrue(value.equals("fba"));
		
		try{
			value = paramMap.get("any");
		}catch(Exception ex) {
			Assert.isTrue(ex instanceof BindingException);
		}
	}

	private interface ITestBean {
		public List<String> getStringList(@Param("name") String name);
	}
}
