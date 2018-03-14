package com.greatbee.core.db.mysql.testcase;

import com.greatbee.base.bean.DBException;
import com.greatbee.base.bean.DataList;
import com.greatbee.core.bean.view.Condition;
import com.greatbee.core.db.mysql.baseCase.BaseConditionTest;

/**
 * Created by usagizhang on 18/3/14.
 */
public class TestListByCondition extends BaseConditionTest {
    public TestListByCondition() throws DBException {
    }

    @Override
    protected void runTest(Condition queryCondition) throws DBException {
        DataList dataList = this.mysqlDataManager.list(mainView.getOi(), mainView.getFields(), queryCondition);
        printJSONObject(dataList);
    }
}
