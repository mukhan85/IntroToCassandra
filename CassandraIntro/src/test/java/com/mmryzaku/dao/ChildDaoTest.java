package com.mmryzaku.dao;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mmryzaku.dao.ChildDao;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;

public class ChildDaoTest {
	private static final Logger log = LoggerFactory.getLogger(ChildDaoTest.class);
	ChildDao childDao ;
	
	@Before
	public void setup() {
		childDao = new ChildDao();
	}
	
	@Test
	public void shouldReadSingleRecord() throws ConnectionException {
		ColumnList<String> result = childDao.read("mukhan.myrzakulov");
		log(result);
	}

	private void log(ColumnList<String> columns) {
        for (Column<String> column : columns) {                          
            log.info("[" + column.getName() + "]->[" + column.getStringValue() + "]");
        }
    }
	
	
	@Test
	public void shouldInsertSingleRecord() throws ConnectionException {
		Map<String, String> childToInsert = new HashMap<String, String>();
		childToInsert.put("country", "Kazakhstan");
		childToInsert.put("firstname", "Mukhan");
		childToInsert.put("lastname", "Myrzakulov");
		childToInsert.put("state", "NY");
		childToInsert.put("currentfocus", "Cassandra");
		childDao.write("mukhan.myrzakulov", childToInsert);
	}
	
	@Test
	public void shouldDeleteSingleRecord() throws ConnectionException {
		String rowKey = "mukhan.myrzakulov";
		childDao.delete(rowKey);
		ColumnList<String> deletedRecord = childDao.read(rowKey);
		log(deletedRecord);
	}
}
