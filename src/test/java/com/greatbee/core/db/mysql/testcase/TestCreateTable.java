package com.greatbee.core.db.mysql.testcase;

import java.util.ArrayList;
import java.util.List;

import com.greatbee.base.bean.DBException;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.db.mysql.MysqlSchemaDataManagerTest;
import com.greatbee.core.bean.constant.DT;
import com.greatbee.core.bean.oi.Field;

public class TestCreateTable extends MysqlSchemaDataManagerTest implements ExceptionCode {

    private OI oi;
    private List<Field> dFields;

    public void testCreateTable() throws DBException {
        //初始化
        this.setUp();
        this.init();
        this.mysqlDataManager.createTable(oi, dFields);
    }

    private void init() {
        oi = this.initOI("测试表", "tb_test", "tb_test");
        dFields = new ArrayList<Field>();
        dFields.add(this.initField(oi, "id", "id", DT.INT, 11, true));
        dFields.add(this.initField(oi, "name", "name", DT.String, 128, true));
        dFields.add(this.initField(oi, "remark", "remark", DT.String, 256, true));
        dFields.add(this.initField(oi, "createDate", "createDate", DT.Time, 0));
    }
}