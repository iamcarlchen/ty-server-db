package com.greatbee.core.db.mysql.testcase;

import com.greatbee.base.bean.DBException;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.db.mysql.MysqlSchemaDataManagerTest;
import org.junit.Test;


public class TestDiff extends MysqlSchemaDataManagerTest implements ExceptionCode {
    
    @Test
    public void testDiffByDeffault() throws DBException {
        //初始化
        this.setUp();
    
    }

}