package com.greatbee.core.db.mysql.testcase;

import com.greatbee.base.bean.DBException;
import com.greatbee.base.bean.DataPage;
import com.greatbee.core.bean.view.Condition;
import com.greatbee.core.db.mysql.baseCase.BaseConditionTest;

/**
 * Created by usagizhang on 18/3/14.
 */
public class TestPageByCondition extends BaseConditionTest {
    public TestPageByCondition() throws DBException {

    }

    @Override
    protected void runTest(Condition queryCondition) throws DBException {
        DataPage dataPage = this.mysqlDataManager.page(mainView.getOi(), mainView.getFields(), 1, 5, queryCondition);
        printJSONObject(dataPage);
    }
}
