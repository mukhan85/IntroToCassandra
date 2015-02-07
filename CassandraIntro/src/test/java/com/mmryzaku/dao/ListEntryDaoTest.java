package com.mmryzaku.dao;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mmryzaku.domain.ListEntry;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;

public class ListEntryDaoTest {
	private static final Logger log = LoggerFactory.getLogger(ListEntryDaoTest.class);
	
	ListEntryDao listDao;
	
	@Before
	public void setup() {
		listDao = new ListEntryDao();
	}
	
	@Test
	public void shouldRetrieveSingleRecord() throws ConnectionException {
		ColumnList<ListEntry> result = listDao.read("nice:USA");
		log(result);
	}

    private void log(ColumnList<ListEntry> columns) {
        for (Column<ListEntry> column : columns) {
            ListEntry listEntry = column.getName();
            log.debug("---");
            log.debug("listEntry.state=>[" + listEntry.state + "]");
            log.debug("listEntry.zip=>[" + listEntry.zip + "]");
            log.debug("listEntry.childId=>[" + listEntry.childId + "]");
        }
    }
    
    @Test
    public void shouldInsertSingleRecord() throws ConnectionException {
        ListEntry listEntry = new ListEntry();
        listEntry.state = "PA";
        listEntry.zip = "19464";
        listEntry.childId = "hank";
        listDao.write("nice:USA", listEntry);
    }
}
