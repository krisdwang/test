package com.amazon.fba.mic.dao.utils;

import static org.junit.Assert.fail;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.ibatis.type.JdbcType;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.anyInt;

public class BooleanToCharTypeHandlerTest {
	
	private BooleanToCharTypeHandler typeHandler;
	private ResultSet rs = null;
	private PreparedStatement ps = null;
	private CallableStatement cs = null;
	private JdbcType jdbcType = null;
	
	@Before
	public void setUp() throws Exception {
		typeHandler = new BooleanToCharTypeHandler();
		rs = EasyMock.createMock(ResultSet.class);
		ps = EasyMock.createMock(PreparedStatement.class);
		cs = EasyMock.createMock(CallableStatement.class);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testSetNonNullParameterPreparedStatementIntBooleanJdbcType() throws SQLException {
		boolean value = true;
		ps.setObject(0, value == true ? "Y" : "N", JdbcType.forCode(Types.CHAR).TYPE_CODE);
		EasyMock.expectLastCall();
		EasyMock.replay(ps);
		typeHandler.setNonNullParameter(ps, 0, value, JdbcType.forCode(Types.CHAR));		
	}
	
	@Test
	public void testSetNonNullParameterPreparedStatementIntBooleanJdbcType2() throws SQLException {		
		boolean value = false;
		ps.setString(0, value == true ? "Y" : "N");
		EasyMock.expectLastCall();
		EasyMock.replay(ps);
		typeHandler.setNonNullParameter(ps, 0, value, null);		
	}

	@Test
	public void testGetNullableResultResultSetString() throws SQLException {
		EasyMock.expect(rs.getString(isA(String.class))).andReturn("Y").anyTimes();		
		EasyMock.replay(rs);
		typeHandler.getNullableResult(rs, "c1");
	}

	@Test
	public void testGetNullableResultResultSetInt() throws SQLException  {
		EasyMock.expect(rs.getString(anyInt())).andReturn("N").anyTimes();		
		EasyMock.replay(rs);
		typeHandler.getNullableResult(rs, new Integer(1));
	}

	@Test
	public void testGetNullableResultCallableStatementInt() throws SQLException   {
		EasyMock.expect(cs.getString(anyInt())).andReturn("N").anyTimes();		
		EasyMock.replay(cs);
		typeHandler.getNullableResult(cs, new Integer(1));
	}

}
