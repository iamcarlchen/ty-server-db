package com.greatbee.core.db.mysql.testcase;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSONObject;
import com.greatbee.base.bean.DBException;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.bean.constant.DT;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.db.mysql.MysqlSchemaDataManagerTest;

import org.junit.Test;

public class TestCreateTable extends MysqlSchemaDataManagerTest implements ExceptionCode {

    private OI oi;
    private List<Field> dFields;

    /**
     * 初始化
     */
    public TestCreateTable() {
        try {
            this.setUp();
            this.init();
        } catch (DBException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCreateTableByDefault() throws DBException {

        this.mysqlDataManager.createTable(this.oi, this.dFields);
    }

    /**
     * 初始化数据
     */
    public void init() throws DBException {
        try {
            this.initConn();
            this.executeQuery(conn, ps, "DROP TABLE IF EXISTS `tb_test`;");
            this.releaseConn();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        this.oi = this.initOI("测试表", "tb_test", "tb_test");
        System.out.println(JSONObject.toJSONString(oi));
        this.dFields = new ArrayList<Field>();
        this.dFields.add(this.initField(oi, "id", "id", DT.INT, 11, true));
        this.dFields.add(this.initField(oi, "name", "name", DT.String, 128));
        this.dFields.add(this.initField(oi, "remark", "remark", DT.String, 256));
        this.dFields.add(this.initField(oi, "delete", "delete", DT.Boolean, 0));
        this.dFields.add(this.initField(oi, "createDate", "createDate", DT.Time, 0));
    }
}