package com.amazon.fba.mic.dao.utils;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;

import amazon.platform.types.CurrencyUnit;

/**
 * Type Handler for CurrencyUnit
 * @author wdong
 *
 */
public class CurrencyUnitTypeHandler extends BaseTypeHandler<CurrencyUnit> {

	public void setNonNullParameter(PreparedStatement ps, int i,
			CurrencyUnit unit, JdbcType jdbcType) throws SQLException {
		if(jdbcType == null) {
			ps.setString(i, unit.getCode());
		} else {
			ps.setObject(i, unit.getCode(), jdbcType.TYPE_CODE);
		}
	}

	public CurrencyUnit getNullableResult(ResultSet rs, String columnName)
			throws SQLException {
		String code = rs.getString(columnName);
		return CurrencyUnit.getInstance(code);
	}

	public CurrencyUnit getNullableResult(ResultSet rs, int columnIndex)
			throws SQLException {
		String code = rs.getString(columnIndex);
		return CurrencyUnit.getInstance(code);
	}

	public CurrencyUnit getNullableResult(CallableStatement cs, int columnIndex)
			throws SQLException {
		String code = cs.getString(columnIndex);
		return CurrencyUnit.getInstance(code);
	}

}
