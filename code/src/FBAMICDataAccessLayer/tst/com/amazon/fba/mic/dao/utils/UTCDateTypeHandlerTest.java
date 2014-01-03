package com.amazon.fba.mic.dao.utils;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.ibatis.type.JdbcType;
import org.easymock.EasyMock;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.anyInt;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class UTCDateTypeHandlerTest {
	
	private UTCDateTypeHandler typeHandler;
	
	private ResultSet rs = null;
	private PreparedStatement ps = null;
	private CallableStatement cs = null;
	private JdbcType jdbcType = null;

	@Before
	public void setUp() throws Exception {
		typeHandler = new UTCDateTypeHandler();
		rs = EasyMock.createMock(ResultSet.class);
		ps = EasyMock.createMock(PreparedStatement.class);
		cs = EasyMock.createMock(CallableStatement.class);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testSetNonNullParameterPreparedStatementIntDateJdbcType() throws SQLException {
		Date date = new Date();
		ps.setTimestamp(anyInt(), isA(Timestamp.class), isA(Calendar.class));
		EasyMock.expectLastCall();
		EasyMock.replay(ps);
		typeHandler.setNonNullParameter(ps, 0, date, JdbcType.forCode(Types.TIMESTAMP));		
	}

	@Test
	public void testGetNullableResultResultSetString() throws SQLException {
		EasyMock.expect(rs.getTimestamp(isA(String.class), isA(Calendar.class))).andReturn(new Timestamp(new Date().getTime())).anyTimes();
		EasyMock.replay(rs);
		Date date = typeHandler.getNullableResult(rs, "time");		
		Assert.assertNotNull(date);
	}

	@Test
	public void testGetNullableResultResultSetInt() throws SQLException {
		EasyMock.expect(rs.getTimestamp(anyInt(), isA(Calendar.class))).andReturn(new Timestamp(new Date().getTime())).anyTimes();
		EasyMock.replay(rs);
		Date date = typeHandler.getNullableResult(rs, 1);		
		Assert.assertNotNull(date);
	}

	@Test
	public void testGetNullableResultCallableStatementInt() throws SQLException {
		EasyMock.expect(cs.getTimestamp(anyInt(), isA(Calendar.class))).andReturn(new Timestamp(new Date().getTime())).anyTimes();
		EasyMock.replay(cs);
		Date date = typeHandler.getNullableResult(cs, 1);		
		Assert.assertNotNull(date);
	}

}
