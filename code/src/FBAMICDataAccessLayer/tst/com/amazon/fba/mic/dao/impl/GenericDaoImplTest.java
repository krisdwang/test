package com.amazon.fba.mic.dao.impl;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.isA;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import amazon.SSOF.common.util.ReflectionUtils;

import com.amazon.fba.mic.dao.AbstractDomainObject;
import com.amazon.fba.mic.dao.AbstractPrimaryKey;
import com.amazon.fba.mic.dao.GenericDao;
import com.amazon.fba.mic.dao.namingstrategy.NamingStrategy;
import com.amazon.fba.mic.dao.namingstrategy.impl.NamingStrategyImpl;

public class GenericDaoImplTest {
	
	private GenericDaoImpl<TestDomainObject, TestPrimaryKey> genericDaoImpl = null;
	private SqlSession sqlSession = null;
	private Configuration configuration = null;
	private NamingStrategy namingStrategy = null;

	@Before
	public void setUp() throws Exception {
		sqlSession = EasyMock.createMock(SqlSession.class);		
		configuration = EasyMock.createMock(Configuration.class);
		this.namingStrategy = new NamingStrategyImpl();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testInitDao() throws Exception {
		this.genericDaoImpl = new GenericDaoImpl<TestDomainObject, TestPrimaryKey> ();
		
		EasyMock.expect(this.sqlSession.getConfiguration()).andReturn(configuration).anyTimes();
		configuration.addMapper(TestDomainObject.class);
		EasyMock.expectLastCall();
		EasyMock.replay(configuration);
		EasyMock.replay(sqlSession);
		
		this.genericDaoImpl.initDao();				
	}
	
	@Test
	public void testInitDaoWithMapper() throws Exception {
		this.genericDaoImpl = new GenericDaoImpl<TestDomainObject, TestPrimaryKey>(TestMapper.class);
		EasyMock.expect(this.sqlSession.getConfiguration()).andReturn(configuration).anyTimes();
		configuration.addMapper(TestMapper.class);
		EasyMock.expectLastCall();
		ReflectionUtils.setFieldValue(this.genericDaoImpl, "sqlSession", this.sqlSession);
		EasyMock.replay(configuration);
		EasyMock.replay(sqlSession);
		this.genericDaoImpl.initDao();
	}

	@Test
	public void testGenericDaoImpl() {
		this.genericDaoImpl = new GenericDaoImpl<TestDomainObject, TestPrimaryKey> ();
		Assert.assertNotNull(genericDaoImpl);
	}

	@Test
	public void testGenericDaoImplClassOfT() {
		this.genericDaoImpl = new GenericDaoImpl<TestDomainObject, TestPrimaryKey> (TestDomainObject.class);
		Assert.assertNotNull(genericDaoImpl);
		
	}

	@Test
	public void testGetNamingStrategy() {
		this.genericDaoImpl = new GenericDaoImpl<TestDomainObject, TestPrimaryKey> ();
		this.genericDaoImpl.setNamingStrategy(this.namingStrategy);
		Assert.assertNotNull(this.genericDaoImpl.getNamingStrategy());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testExecuteFinder() throws Exception {
		this.genericDaoImpl = new GenericDaoImpl<TestDomainObject, TestPrimaryKey> (TestDomainObject.class);
		this.genericDaoImpl.setNamingStrategy(namingStrategy);
		Object[] queryArgs = new Object[] {"US"};
		List<Object> list = new ArrayList<Object>();
		list.add(new TestDomainObject());
		EasyMock.expect(this.sqlSession.selectList(isA(String.class),anyObject())).andReturn(list).anyTimes();
		EasyMock.expect(this.sqlSession.selectOne(isA(String.class), anyObject())).andReturn(new TestDomainObject()).anyTimes();
		EasyMock.replay(configuration);
		EasyMock.replay(sqlSession);
		ReflectionUtils.setFieldValue(this.genericDaoImpl, "sqlSession", this.sqlSession);
		Method method1 = TestDao.class.getMethod("listDomains", String.class);	
		List<TestDomainObject> domains = (List<TestDomainObject>)this.genericDaoImpl.executeFinder(method1, queryArgs);
		Assert.assertNotNull(domains);
		Assert.assertNotNull(domains.get(0));
		
		Method method2 = TestDao.class.getMethod("getDomain", Long.class);
		queryArgs = new Object[] {1L};
		TestDomainObject domain = (TestDomainObject)this.genericDaoImpl.executeFinder(method2, queryArgs);
		Assert.assertNotNull(domain);
	}

	@Test
	public void testCreate() throws IllegalAccessException {		
		this.genericDaoImpl = new GenericDaoImpl<TestDomainObject, TestPrimaryKey> (TestDomainObject.class);
		EasyMock.expect(sqlSession.insert(isA(String.class), isA(Object.class))).andReturn(1).anyTimes();
		EasyMock.replay(configuration);
		EasyMock.replay(sqlSession);
		ReflectionUtils.setFieldValue(this.genericDaoImpl, "sqlSession", this.sqlSession);
		TestDomainObject domain = new TestDomainObject();
		int row = this.genericDaoImpl.create(domain);		
		Assert.assertEquals(row, 1);
		Assert.assertEquals(domain.isPersisted(), true);
	}

	@Test
	public void testCountT() throws Exception {
		this.genericDaoImpl = new GenericDaoImpl<TestDomainObject, TestPrimaryKey> (TestDomainObject.class);
		EasyMock.expect(this.sqlSession.selectOne(isA(String.class), isA(Object.class))).andReturn(10L).anyTimes();
		EasyMock.replay(configuration);
		EasyMock.replay(sqlSession);
		ReflectionUtils.setFieldValue(this.genericDaoImpl, "sqlSession", this.sqlSession);
		long count = this.genericDaoImpl.count(new TestDomainObject());
		Assert.assertEquals(count, 10L);
	}

	@Test
	public void testCountMapOfQQ() throws Exception {
		this.genericDaoImpl = new GenericDaoImpl<TestDomainObject, TestPrimaryKey> (TestDomainObject.class);
		EasyMock.expect(this.sqlSession.selectOne(isA(String.class), isA(Map.class))).andReturn(10L).anyTimes();
		EasyMock.replay(configuration);
		EasyMock.replay(sqlSession);
		ReflectionUtils.setFieldValue(this.genericDaoImpl, "sqlSession", this.sqlSession);
		long count = this.genericDaoImpl.count(new HashMap<String,String>());
		Assert.assertEquals(count, 10L);
	}

	@Test
	public void testRetrieve() throws Exception {
		this.genericDaoImpl = new GenericDaoImpl<TestDomainObject, TestPrimaryKey> (TestDomainObject.class);
		EasyMock.expect(this.sqlSession.selectOne(isA(String.class),isA(AbstractPrimaryKey.class))).andReturn(new TestDomainObject()).anyTimes();
		EasyMock.replay(configuration);
		EasyMock.replay(sqlSession);
		ReflectionUtils.setFieldValue(this.genericDaoImpl, "sqlSession", this.sqlSession);
		TestDomainObject domain = this.genericDaoImpl.retrieve(new TestPrimaryKey());
		Assert.assertNotNull(domain);
		Assert.assertEquals(domain.isPersisted(), true);
	}

	@Test
	public void testQuery() throws Exception  {
		this.genericDaoImpl = new GenericDaoImpl<TestDomainObject, TestPrimaryKey> (TestDomainObject.class);
		List<Object> list = new ArrayList<Object>();
		list.add(new TestDomainObject());
		EasyMock.expect(this.sqlSession.selectList(isA(String.class),isA(AbstractDomainObject.class))).andReturn(list).anyTimes();
		EasyMock.replay(configuration);
		EasyMock.replay(sqlSession);
		ReflectionUtils.setFieldValue(this.genericDaoImpl, "sqlSession", this.sqlSession);
		List<TestDomainObject> domains = this.genericDaoImpl.query(new TestDomainObject());
		Assert.assertNotNull(domains);
		Assert.assertEquals(domains.size(), 1);
		Assert.assertEquals(domains.get(0).isPersisted(), true);
	}

	@Test
	public void testUpdateT() throws Exception {
		this.genericDaoImpl = new GenericDaoImpl<TestDomainObject, TestPrimaryKey> (TestDomainObject.class);
		EasyMock.expect(this.sqlSession.update(isA(String.class), isA(AbstractDomainObject.class))).andReturn(1).anyTimes();
		EasyMock.replay(configuration);
		EasyMock.replay(sqlSession);
		ReflectionUtils.setFieldValue(this.genericDaoImpl, "sqlSession", this.sqlSession);
		int row = this.genericDaoImpl.update(new TestDomainObject());
		Assert.assertEquals(row, 1);
	}

	@Test
	public void testUpdateMapOfQQ()  throws Exception {
		this.genericDaoImpl = new GenericDaoImpl<TestDomainObject, TestPrimaryKey> (TestDomainObject.class);
		NamingStrategy strategy = EasyMock.createMock(NamingStrategy.class);
		EasyMock.expect(strategy.queryNameFromMethod(isA(Class.class), isA(Method.class))).andReturn("updateBy").anyTimes();
		this.genericDaoImpl.setNamingStrategy(strategy);
		EasyMock.expect(this.sqlSession.update(isA(String.class), isA(HashMap.class))).andReturn(1).anyTimes();		
		EasyMock.replay(strategy);
		EasyMock.replay(configuration);
		EasyMock.replay(sqlSession);
		ReflectionUtils.setFieldValue(this.genericDaoImpl, "sqlSession", this.sqlSession);
		int row = this.genericDaoImpl.internalUpdate(TestDao.class.getMethod("updateBy"), new HashMap<String,String>());
		Assert.assertEquals(row, 1);
	}

	@Test
	public void testDeleteT() throws Exception {
		this.genericDaoImpl = new GenericDaoImpl<TestDomainObject, TestPrimaryKey> (TestDomainObject.class);
		
		TestDomainObject domain = new TestDomainObject();
		EasyMock.expect(this.sqlSession.delete(isA(String.class),isA(AbstractDomainObject.class))).andReturn(1).anyTimes();
		EasyMock.replay(configuration);	
		EasyMock.replay(sqlSession);

		ReflectionUtils.setFieldValue(this.genericDaoImpl, "sqlSession", this.sqlSession);
		this.genericDaoImpl.delete(domain);		
	}

	@Test
	public void testDeleteMapOfQQ() throws Exception  {
		this.genericDaoImpl = new GenericDaoImpl<TestDomainObject, TestPrimaryKey> (TestDomainObject.class);
		Map<String,Object> domain = new HashMap<String,Object>();
		NamingStrategy strategy = EasyMock.createMock(NamingStrategy.class);
		EasyMock.expect(strategy.queryNameFromMethod(isA(Class.class), isA(Method.class))).andReturn("deleteBy").anyTimes();
		this.genericDaoImpl.setNamingStrategy(strategy);
		EasyMock.expect(this.sqlSession.delete(isA(String.class),isA(Map.class))).andReturn(1).anyTimes();
		EasyMock.replay(strategy);
		EasyMock.replay(configuration);	
		EasyMock.replay(sqlSession);
		ReflectionUtils.setFieldValue(this.genericDaoImpl, "sqlSession", this.sqlSession);
		this.genericDaoImpl.internalDelete(TestDao.class.getMethod("deleteBy"), new HashMap<String,String>());		
	}

	@Test
	public void testCurrentSql() {
		/*
		this.genericDaoImpl = new GenericDaoImpl<TestDomainObject, TestPrimaryKey>(TestDomainObject.class);
		MappedStatement ms = EasyMock.createMock(MappedStatement.class);
		EasyMock.expect(this.configuration.getMappedStatement(isA(String.class))).andReturn(ms).anyTimes();
		BoundSql boundSql = EasyMock.createMock(BoundSql.class);
		EasyMock.expect(ms.getBoundSql(anyObject())).andReturn(boundSql).anyTimes();
		EasyMock.expect(boundSql.getSql()).andReturn("SELECT * FROM TABLE WHERE NAME = ? ").anyTimes();
		
		EasyMock.replay(this.configuration);
		EasyMock.replay(this.sqlSession);
		EasyMock.replay(ms);
		EasyMock.replay(boundSql);
		
		this.genericDaoImpl.currentSql("test", new Object[] {"amazon"});
		*/
	}

	@Test
	public void testBatch() throws Exception {
		this.genericDaoImpl = new GenericDaoImpl<TestDomainObject, TestPrimaryKey> (TestDomainObject.class);
		EasyMock.expect(sqlSession.insert(isA(String.class), isA(AbstractDomainObject.class))).andReturn(1).anyTimes();
		EasyMock.replay(configuration);
		EasyMock.replay(sqlSession);
		ReflectionUtils.setFieldValue(this.genericDaoImpl, "sqlSession", this.sqlSession);
		TestDomainObject[] domains = {new TestDomainObject(), new TestDomainObject()}; 
		this.genericDaoImpl.batch(domains);
	}

	private class TestDomainObject extends AbstractDomainObject<TestPrimaryKey> {
		
	}
	
	private class TestPrimaryKey extends AbstractPrimaryKey {
		
	}
	
	private interface TestMapper {
		
	}
	
	private interface TestDao extends GenericDao<TestDomainObject, TestPrimaryKey> {
		public List<TestDomainObject> listDomains(String realm);
		public TestDomainObject getDomain(Long id);
		public void updateBy();
		public void deleteBy();
	}
}
