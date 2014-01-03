package com.amazon.fba.mic.dao.defect;

import com.amazon.fba.mic.dao.GenericDao;
import com.amazon.fba.mic.dao.route.DynaDS;

@DynaDS(datasource = "FBA1DS")
public interface MockedDefectDao extends GenericDao<MockedDefect, MockedDefectPrimaryKey> {
	
}
