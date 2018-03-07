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

public class TestAddField extends MysqlSchemaDataManagerTest implements ExceptionCode {

    /**
     * 初始化
     */
    public TestAddField() {
        this.setUp();
        this.init();
    }

    @Test
    public void testAddFieldByDefault() throws DBException {
        //测试添加字段
        OI oi = this.initOI("测试OI", "tb_test_add_field", "tb_test_add_field");
        Field add_int_field = this.initField(oi, "add_int", "add_int", DT.INT, 11);
        this.mysqlDataManager.addField(oi, add_int_field);
        Field add_string_field = this.initField(oi, "add_string", "add_string", DT.String, 32);
        this.mysqlDataManager.addField(oi, add_string_field);
        Field add_time_field = this.initField(oi, "add_time", "add_time", DT.Time, 0);
        this.mysqlDataManager.addField(oi, add_time_field);
        Field add_date_field = this.initField(oi, "add_date", "add_date", DT.Date, 0);
        this.mysqlDataManager.addField(oi, add_date_field);
        Field add_boolean_field = this.initField(oi, "add_boolean", "add_boolean", DT.Boolean, 0);
        this.mysqlDataManager.addField(oi, add_boolean_field);
        Field add_double_field = this.initField(oi, "add_double", "add_double", DT.Double, 0);
        this.mysqlDataManager.addField(oi, add_double_field);

    }

    /**
     * 初始化测试数据
     */
    public void init() {
        try {
            this.initConn();
            this.executeQuery(this.conn, this.ps, "DROP TABLE IF EXISTS `tb_test_add_field`;");
            StringBuilder queryBuilder = new StringBuilder();
            queryBuilder.append("CREATE TABLE `tb_test_add_field` (");
            queryBuilder.append("`id` int(11) NOT NULL AUTO_INCREMENT,");
            queryBuilder.append("`remark` varchar(256) DEFAULT NULL,");
            queryBuilder.append("`alias` varchar(64) DEFAULT NULL,");
            queryBuilder.append("`name` varchar(64) DEFAULT NULL,");
            queryBuilder.append("PRIMARY KEY (`id`),");
            queryBuilder.append("UNIQUE KEY `id_UNIQUE` (`id`)");
            queryBuilder.append(") ENGINE=InnoDB  DEFAULT CHARSET=utf8");
            this.executeQuery(this.conn, this.ps, queryBuilder.toString());
            this.releaseConn();

        } catch (DBException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}