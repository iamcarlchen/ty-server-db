package com.greatbee.core.db.mysql.testcase;

import com.greatbee.base.bean.DBException;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.db.mysql.baseCase.BasePKFieldTest;

/**
 * Created by usagizhang on 18/3/14.
 */
public class TestDeleteByPK extends BasePKFieldTest {

    public TestDeleteByPK() throws DBException {
    }

    @Override
    protected void runTest(Field pkField) throws DBException {
        this.mysqlDataManager.delete(mainView.getOi(), pkField);
    }
}
