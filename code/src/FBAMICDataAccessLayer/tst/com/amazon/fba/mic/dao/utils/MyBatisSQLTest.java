package com.amazon.fba.mic.dao.utils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MyBatisSQLTest {
	
	private MyBatisSQL myBatisSQL;
	private static final String SQL = "SELECT * FROM TABLEA WHERE NAME=? and GENDER=?";
	private static final String EXPECT_SQL = "SELECT * FROM TABLEA WHERE NAME=amazon and GENDER=male";
	private static final String EXPECT_SQL_WITH_EMPTY_PARAMETER = "SELECT * FROM TABLEA WHERE NAME=amazon and GENDER=?";
	private static final String EMPTY_SQL = "";

	@Before
	public void setUp() throws Exception {
		myBatisSQL = new MyBatisSQL();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testSetSql() {
		this.myBatisSQL.setSql(SQL);
		Assert.assertEquals(this.myBatisSQL.getSql(), SQL);
	}

	@Test
	public void testGetSql() {
		this.myBatisSQL.setSql(SQL);
		Assert.assertEquals(this.myBatisSQL.getSql(), SQL);
	}

	@Test
	public void testSetParameters() {
		this.myBatisSQL.setParameters(new Object[] {"amazon","male"});
		Assert.assertNotNull(this.myBatisSQL.getParameters());
	}

	@Test
	public void testGetParameters() {
		this.myBatisSQL.setParameters(new Object[]{"amazon","male"});
		Assert.assertNotNull(this.myBatisSQL.getParameters());
	}

	@Test
	public void testToString() {
		this.myBatisSQL.setSql(SQL);
		this.myBatisSQL.setParameters(new Object[] {"amazon","male"});
		String sql = this.myBatisSQL.toString();
		System.out.println(sql);
		Assert.assertEquals(sql, EXPECT_SQL);
	}
	
	@Test
	public void testToStringNoParameters() {
		this.myBatisSQL.setSql(SQL);		
		String sql = this.myBatisSQL.toString();
		System.out.println(sql);
		Assert.assertEquals(sql, EMPTY_SQL);
	}
	
	@Test
	public void testToStringWithEmptyParameters() {
		this.myBatisSQL.setSql(SQL);		
		this.myBatisSQL.setParameters(new Object[] {"amazon", null});
		String sql = this.myBatisSQL.toString();
		System.out.println(sql);
		Assert.assertEquals(sql, EXPECT_SQL_WITH_EMPTY_PARAMETER);
	}

}
