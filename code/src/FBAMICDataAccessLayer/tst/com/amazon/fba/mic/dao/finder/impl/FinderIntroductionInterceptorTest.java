package com.amazon.fba.mic.dao.finder.impl;

import static org.easymock.EasyMock.isA;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.aopalliance.intercept.MethodInvocation;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.util.Assert;

import com.amazon.fba.mic.dao.AbstractDomainObject;
import com.amazon.fba.mic.dao.AbstractPrimaryKey;
import com.amazon.fba.mic.dao.GenericDao;
import com.amazon.fba.mic.dao.executor.DeleteExecutor;
import com.amazon.fba.mic.dao.executor.InternalDeleteExecutor;
import com.amazon.fba.mic.dao.executor.InternalUpdateExecutor;
import com.amazon.fba.mic.dao.executor.UpdateExecutor;
import com.amazon.fba.mic.dao.finder.FinderExecutor;


public class FinderIntroductionInterceptorTest {
	
	
	private MethodInvocation invocation;
	private FinderExecutor<?> executor;
	private UpdateExecutor<TestDomainObject> updater;
	private InternalUpdateExecutor internalUpdater;
	private DeleteExecutor<TestDomainObject> deleter;
	private InternalDeleteExecutor internalDeleter;
	private FinderIntroductionInterceptor<?> interceptor;
	
	private List<TestDomainObject> list = new ArrayList<TestDomainObject>();
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Before
	public void setUp() throws Exception {				
		list.add(new TestDomainObject());
		interceptor = new FinderIntroductionInterceptor();	
		invocation = EasyMock.createMock(MethodInvocation.class);
		executor = EasyMock.createMock(FinderExecutor.class);	
		updater = EasyMock.createMock(UpdateExecutor.class);
		deleter = EasyMock.createMock(DeleteExecutor.class);
		internalUpdater = EasyMock.createMock(InternalUpdateExecutor.class);
		internalDeleter = EasyMock.createMock(InternalDeleteExecutor.class);
	}


	@After
	public void tearDown() throws Exception {
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testInvokeList() throws Throwable {
		Method[] methods = TestDao.class.getMethods();		
		Method method = null;		
		for(Method m : methods) {
			if(m.getName().equals("listDomainObjects")) {
				method = m;
				break;
			}
		}
				
		EasyMock.expect(invocation.getMethod()).andReturn(method).anyTimes();
		EasyMock.expect(invocation.getArguments()).andReturn(new Object[] {""}).anyTimes();		
		EasyMock.expect(invocation.getThis()).andReturn(executor).anyTimes();
		
		EasyMock.expect(executor.executeFinder(isA(Method.class),isA(Object[].class))).andReturn(list).anyTimes();
		
		EasyMock.replay(invocation);
		EasyMock.replay(executor);
		List<TestDomainObject> objects = (List<TestDomainObject>)interceptor.invoke(invocation);
		
		Assert.isTrue(objects.size() == 1);
	}
	
	@Test
	public void testInvokeGet() throws Throwable {
		Method[] methods = TestDao.class.getMethods();		
		Method method = null;		
		for(Method m : methods) {
			if(m.getName().equals("getDomainObjects")) {
				method = m;
				break;
			}
		}
				
		EasyMock.expect(invocation.getMethod()).andReturn(method).anyTimes();
		EasyMock.expect(invocation.getArguments()).andReturn(new Object[] {""}).anyTimes();		
		EasyMock.expect(invocation.getThis()).andReturn(executor).anyTimes();
		
		EasyMock.expect(executor.executeFinder(isA(Method.class),isA(Object[].class))).andReturn(new TestDomainObject()).anyTimes();
		
		EasyMock.replay(invocation);
		EasyMock.replay(executor);
		TestDomainObject obj = (TestDomainObject)interceptor.invoke(invocation);
		
		Assert.isTrue(obj != null);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testInvokeFind() throws Throwable {
		Method[] methods = TestDao.class.getMethods();		
		Method method = null;		
		for(Method m : methods) {
			if(m.getName().equals("findDomainObjects")) {
				method = m;
				break;
			}
		}
				
		EasyMock.expect(invocation.getMethod()).andReturn(method).anyTimes();
		EasyMock.expect(invocation.getArguments()).andReturn(new Object[] {""}).anyTimes();		
		EasyMock.expect(invocation.getThis()).andReturn(executor).anyTimes();
		
		EasyMock.expect(executor.executeFinder(isA(Method.class),isA(Object[].class))).andReturn(list).anyTimes();
		
		EasyMock.replay(invocation);
		EasyMock.replay(executor);
		List<TestDomainObject> objects = (List<TestDomainObject>)interceptor.invoke(invocation);
		
		Assert.isTrue(objects.size() == 1);
	}

	@Test
	public void testCreate() throws Throwable {
		Method[] methods = TestDao.class.getMethods();		
		Method method = null;		
		for(Method m : methods) {
			if(m.getName().equals("create")) {
				method = m;
				break;
			}
		}
		
		EasyMock.expect(invocation.getMethod()).andReturn(method).anyTimes();
		EasyMock.expect(invocation.getArguments()).andReturn(new Object[] {""}).anyTimes();		
		EasyMock.expect(invocation.getThis()).andReturn(executor).anyTimes();
		
		EasyMock.expect(invocation.proceed()).andReturn(1).anyTimes();
		
		EasyMock.replay(invocation);
		EasyMock.replay(executor);
		
		int result = (Integer)this.interceptor.invoke(invocation);
		Assert.isTrue(result == 1);
	}
	
	@Test
	public void testUpdate() throws Throwable {		
		Method[] methods = TestDao.class.getMethods();		
		Method method = null;		
		for(Method m : methods) {
			if(m.getName().equals("update")) {
				method = m;
				break;
			}
		}
		
		EasyMock.expect(invocation.getMethod()).andReturn(method).anyTimes();
		EasyMock.expect(invocation.getArguments()).andReturn(new Object[] {new HashMap<String,String>()}).anyTimes();		
		EasyMock.expect(invocation.getThis()).andReturn(this.internalUpdater).anyTimes();
		EasyMock.expect(internalUpdater.internalUpdate(isA(Method.class), isA(Map.class))).andReturn(1).anyTimes();
		
		EasyMock.expect(invocation.proceed()).andReturn(1).anyTimes();
		
		EasyMock.replay(invocation);
		EasyMock.replay(internalUpdater);
		
		int result = (Integer)this.interceptor.invoke(invocation);
		Assert.isTrue(result == 1);
	}
	
	@Test
	public void testUpdateNotMap() throws Throwable {
		Method[] methods = TestDao.class.getMethods();		
		Method method = null;		
		for(Method m : methods) {
			if(m.getName().equals("update")) {
				method = m;
				break;
			}
		}
		
		EasyMock.expect(invocation.getMethod()).andReturn(method).anyTimes();
		EasyMock.expect(invocation.getArguments()).andReturn(new Object[] {new Object()}).anyTimes();		
		EasyMock.expect(invocation.getThis()).andReturn(internalUpdater).anyTimes();
		EasyMock.expect(internalUpdater.internalUpdate(isA(Method.class), isA(Object.class))).andReturn(1).anyTimes();
		
		EasyMock.expect(invocation.proceed()).andReturn(1).anyTimes();
		
		EasyMock.replay(invocation);
		
		EasyMock.replay(internalUpdater);
		
		int result = (Integer)this.interceptor.invoke(invocation);
		Assert.isTrue(result == 1);
	}
	
	@Test
	public void testDelete() throws Throwable {		
		Method[] methods = TestDao.class.getMethods();		
		Method method = null;		
		for(Method m : methods) {
			if(m.getName().equals("delete")) {
				method = m;
				break;
			}
		}
		
		EasyMock.expect(invocation.getMethod()).andReturn(method).anyTimes();
		EasyMock.expect(invocation.getArguments()).andReturn(new Object[] {new HashMap<String,String>()}).anyTimes();		
		EasyMock.expect(invocation.getThis()).andReturn(this.internalDeleter).anyTimes();
		
		EasyMock.expect(invocation.proceed()).andReturn(new Object()).anyTimes();
		
		EasyMock.replay(invocation);
		internalDeleter.internalDelete(isA(Method.class), isA(Map.class));
		EasyMock.expectLastCall();
		EasyMock.replay(deleter);		
		
		this.interceptor.invoke(invocation);
	}
	
	@Test
	public void testDeleteNoMap() throws Throwable {		
		Method[] methods = TestDao.class.getMethods();		
		Method method = null;		
		for(Method m : methods) {
			if(m.getName().equals("delete")) {
				method = m;
				break;
			}
		}
		
		TestDomainObject obj = new TestDomainObject();
		EasyMock.expect(invocation.getMethod()).andReturn(method).anyTimes();
		EasyMock.expect(invocation.getArguments()).andReturn(new Object[] {obj}).anyTimes();		
		EasyMock.expect(invocation.getThis()).andReturn(deleter).anyTimes();
		
		EasyMock.expect(invocation.proceed()).andReturn(new Object()).anyTimes();
		
		EasyMock.replay(invocation);
	    this.deleter.delete(obj);
		EasyMock.expectLastCall();
		EasyMock.replay(deleter);		
		
		this.interceptor.invoke(invocation);
	}
	
	@SuppressWarnings("rawtypes")
	@Test
	public void testCreateWithMap() throws Throwable {
		Method[] methods = TestDao.class.getMethods();		
		Method method = null;		
		for(Method m : methods) {
			if(m.getName().equals("create")) {
				method = m;
				break;
			}
		}
		EasyMock.expect(invocation.getMethod()).andReturn(method).anyTimes();
		EasyMock.expect(invocation.getArguments()).andReturn(new Object[] {new HashMap()}).anyTimes();		
		EasyMock.expect(invocation.getThis()).andReturn(deleter).anyTimes();
		
		EasyMock.expect(invocation.proceed()).andReturn(new Object()).anyTimes();
		EasyMock.replay(invocation);
		EasyMock.replay(executor);
		this.interceptor.invoke(invocation);
	}
	
	
	
	
	private interface TestDao extends GenericDao<TestDomainObject, TestPrimaryKey> {
		public List<TestDomainObject> listDomainObjects();
		public TestDomainObject getDomainObjects();
		public List<TestDomainObject> findDomainObjects();
	}
	
	private class TestDomainObject extends AbstractDomainObject<TestPrimaryKey> {
		
	}
	
	private class TestPrimaryKey extends AbstractPrimaryKey {
		
	}
}
