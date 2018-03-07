package com.greatbee.core.db.mysql.testcase;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.greatbee.base.bean.DBException;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.bean.constant.DT;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.bean.view.DSView;
import com.greatbee.core.bean.view.DiffItem;
import com.greatbee.core.bean.view.OIView;
import com.greatbee.core.db.mysql.MysqlSchemaDataManagerTest;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.mapping.Array;
import org.junit.Test;

public class TestDropTable extends MysqlSchemaDataManagerTest implements ExceptionCode {
    public TestDropTable() {
        this.setUp();
    }

    @Test
    public void testDropTableByDefault() throws DBException {
        this.mysqlDataManager.dropTable(this.initOI("测试表", "ly_base_app", "ly_base_app"));
    }

}