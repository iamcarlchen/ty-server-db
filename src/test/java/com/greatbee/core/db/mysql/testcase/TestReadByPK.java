package com.greatbee.core.db.mysql.testcase;

import com.greatbee.base.bean.DBException;
import com.greatbee.base.bean.Data;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.db.mysql.baseCase.BasePKFieldTest;

/**
 * Created by usagizhang on 18/3/14.
 */
public class TestReadByPK extends BasePKFieldTest {
    public TestReadByPK() throws DBException {
    }

    @Override
    protected void runTest(Field pkField) throws DBException {
        Data data = this.mysqlDataManager.read(mainView.getOi(), mainView.getFields(), pkField);
        printJSONObject(data);
    }
}
