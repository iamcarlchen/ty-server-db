package com.greatbee.core.db.sqlserver;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.greatbee.base.bean.DBException;
import com.greatbee.base.bean.Data;
import com.greatbee.base.util.DataUtil;
import com.greatbee.base.util.RandomGUIDUtil;
import com.greatbee.core.bean.oi.DS;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.view.DSView;
import com.greatbee.core.bean.view.OIView;
import com.greatbee.core.manager.data.DataManagerTest;
import com.greatbee.core.manager.data.DataManagerTestCase;
import com.greatbee.core.manager.data.RelationalDataManager;
import com.greatbee.core.db.sqlserver.testcase.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

/**
 * Oracle Data Manager
 * <p/>
 * Author: CarlChen
 * Date: 2017/11/21
 */
public class SqlServerDataManagerTest extends DataManagerTest implements DataManagerTestCase {

    private final static String TEST_USER_TABLE = "ty_test_user";
    private final static String TEST_ACCOUNT_TABLE = "ty_test_account";
    private OIView TEST_USER_OIVIEW;
    private OIView TEST_ACCOUNT_OIVIEW;


    @Override
    public void initSchema(Connection conn, PreparedStatement ps) throws DBException, SQLException {
        this.dropSchema(conn, ps);
        this.createSchema(conn, ps);
        this.insertTestData(conn, ps);
        this.initTestOIView();
    }

    @Override
    public RelationalDataManager getDataManager() {
        return (RelationalDataManager) context.getBean("sqlServerDataManager");
    }

    @Override
    public DS getDS() throws DBException {
        return dsManager.getDSByAlias("test_sqlserver");
    }

    @Override
    public DSView getDsView() throws DBException {
        return dataManager.exportFromPhysicsDS(dataSource);
    }

    public void dropSchema(Connection conn, PreparedStatement ps) throws SQLException {
        System.out.println("drop schema " + TEST_USER_TABLE);
        StringBuilder schemaBuilder = new StringBuilder();
        schemaBuilder.append("IF EXISTS (  ").append("SELECT * FROM sys.objects   WHERE name = N'").append(TEST_USER_TABLE).append("'").append(")\n");
        schemaBuilder.append("DROP TABLE " + TEST_USER_TABLE + " \n");
        ps = conn.prepareStatement(schemaBuilder.toString());
        ps.execute();
        System.out.println("drop schema " + TEST_USER_TABLE + " done!");

        System.out.println("drop schema " + TEST_ACCOUNT_TABLE);
        schemaBuilder = new StringBuilder();
        schemaBuilder.append("IF EXISTS (  ").append("SELECT * FROM sys.objects   WHERE name = N'").append(TEST_ACCOUNT_TABLE).append("'").append(")\n");
        schemaBuilder.append("DROP TABLE " + TEST_ACCOUNT_TABLE + " \n");
        ps = conn.prepareStatement(schemaBuilder.toString());
        ps.execute();
        System.out.println("drop schema " + TEST_ACCOUNT_TABLE + " done!");
    }

    public void createSchema(Connection conn, PreparedStatement ps) throws SQLException {
        System.out.println("create schema " + TEST_USER_TABLE);
        StringBuilder schemaBuilder = new StringBuilder();
        schemaBuilder.append("create table " + TEST_USER_TABLE + " (");
        schemaBuilder.append("  \"id\" int identity(1,1) primary key,");
        schemaBuilder.append("  \"name\" varchar(64) unique  not null,");
        schemaBuilder.append("  \"alias\" varchar(64) not null,");
        schemaBuilder.append("  \"createDate\" datetime not null,");
        schemaBuilder.append("  \"remark\" varchar(256) ,");
        schemaBuilder.append("  \"age\" int default 1,");
        schemaBuilder.append("  \"desc\" text default ''");
        schemaBuilder.append(")\n");
        ps = conn.prepareStatement(schemaBuilder.toString());
        ps.execute();
        System.out.println("schema done!");

        System.out.println("create schema " + TEST_ACCOUNT_TABLE);
        schemaBuilder = new StringBuilder();
        schemaBuilder.append("create table " + TEST_ACCOUNT_TABLE + " (");
        schemaBuilder.append("  \"id\" int identity(1,1) primary key,");
        schemaBuilder.append("  \"account\" varchar(64) unique  not null,");
        schemaBuilder.append("  \"password\" varchar(64) not null,");
        schemaBuilder.append("  \"userAlias\" varchar(64) not null,");
        schemaBuilder.append("  \"remark\" varchar(256) ,");
        schemaBuilder.append("  \"enableStatus\" int default 1,");
        schemaBuilder.append(")\n");
        ps = conn.prepareStatement(schemaBuilder.toString());
        ps.execute();
        System.out.println("schema done!");
    }

    public void insertTestData(Connection conn, PreparedStatement ps) throws SQLException {
        System.out.println("insert data into " + TEST_USER_TABLE);
        for (int i = 1; i < 100; i++) {
            StringBuilder schemaBuilder = new StringBuilder();
            schemaBuilder.append("INSERT INTO ").append(TEST_USER_TABLE).append(" VALUES ('test_user_" + i + "','user" + i + "','" + DataUtil.formatDateTime(new Date()) + "','" + RandomGUIDUtil.getRawGUID() + "'," + i + ",'" + RandomGUIDUtil.getRawGUID() + "')");
            System.out.println(schemaBuilder.toString());
            ps = conn.prepareStatement(schemaBuilder.toString());
            ps.executeUpdate();
        }
        System.out.println("insert data done!");


        System.out.println("insert data into " + TEST_ACCOUNT_TABLE);
        for (int i = 1; i < 100; i++) {
            StringBuilder schemaBuilder = new StringBuilder();
            schemaBuilder.append("INSERT INTO ").append(TEST_ACCOUNT_TABLE).append(" VALUES ('test_user_" + i + "','" + RandomGUIDUtil.getRawGUID() + "','user" + i + "','',1)");
            System.out.println(schemaBuilder.toString());
            ps = conn.prepareStatement(schemaBuilder.toString());
            ps.executeUpdate();
        }
        System.out.println("insert data done!");

    }

    public void testInitSchema() throws DBException {
        System.out.println("init done!");
    }

    /**
     * 取第一张表作为测试
     *
     * @return
     */
    private void initTestOIView() throws DBException {
        DSView dsView = this.getDSView();
        List<OIView> oiViews = dsView.getOiViews();
        System.out.println("OIViewList -> " + JSONArray.toJSONString(oiViews));
        if (oiViews != null) {
            //选择测试用的OI
            for (OIView item : oiViews) {
                if (item.getOi().getAlias().equalsIgnoreCase(TEST_USER_TABLE)) {
                    TEST_USER_OIVIEW = item;
                } else if (item.getOi().getAlias().equalsIgnoreCase(TEST_ACCOUNT_TABLE)) {
                    TEST_ACCOUNT_OIVIEW = item;
                }
            }
        }
    }

    /**
     * 测试获取单条记录
     *
     * @throws DBException
     */
    public Data getReadByPK() throws DBException {
        OIView oiView = TEST_USER_OIVIEW;
        Field pkField = null;
        List<Field> fields = oiView.getFields();
        for (Field field : fields) {
            if (field.isPk()) {
                pkField = field;
                break;
            }
        }
        pkField.setFieldValue("1");//设置主键值
        Data data = dataManager.read(oiView.getOi(), fields, pkField);
        System.out.println("Data -> " + JSONObject.toJSONString(data));
        return data;
    }


    /**
     * 测试插入单条数据
     *
     * @throws DBException
     */
    public void testCreateData() throws DBException {
        new CreateData(TEST_USER_OIVIEW, dataManager);
    }


    //=======================================单表=======================================

    /**
     * 测试获取列表
     *
     * @throws DBException
     */
    public void testListByConnectorTree() throws DBException {
        new ListByConnectorTree(TEST_USER_OIVIEW, dataManager);
    }

    /**
     * 测试获取列表(condition)
     *
     * @throws DBException
     */
    public void testListByCondition() throws DBException {
        new ListByCondition(TEST_USER_OIVIEW, dataManager);
    }


    /**
     * 测试count函数
     *
     * @throws DBException
     */
    public void testCountByConnectorTree() throws DBException {
        new CountByConnectorTree(TEST_USER_OIVIEW, dataManager);
    }

    /**
     * 测试分页列表读取
     *
     * @throws DBException
     */
    public void testPageByConnectorTree() throws DBException {
        new PageByConnectorTree(TEST_USER_OIVIEW, dataManager);
    }

    /**
     * 测试分页列表读取(Condition)
     *
     * @throws DBException
     */
    public void testPageByCondition() throws DBException {
        new PageByCondition(TEST_USER_OIVIEW, dataManager);
    }


    /**
     * 测试获取单条记录
     *
     * @throws DBException
     */
    public void testReadByConnectorTree() throws DBException {
        new ReadByConnectorTree(TEST_USER_OIVIEW, dataManager);
    }

    /**
     * 测试获取单条记录
     *
     * @throws DBException
     */
    public void testReadByPK() throws DBException {
        new ReadByPK(TEST_USER_OIVIEW, dataManager);
    }

    //=======================================2张表联动=======================================

    /**
     * 测试获取列表
     *
     * @throws DBException
     */
    public void test2TableListByConnectorTree() throws DBException {
        new ListByConnectorTree(TEST_USER_OIVIEW, TEST_ACCOUNT_OIVIEW, dataManager);
    }


    /**
     * 测试count函数
     *
     * @throws DBException
     */
    public void test2TableCountByConnectorTree() throws DBException {
        new CountByConnectorTree(TEST_USER_OIVIEW, TEST_ACCOUNT_OIVIEW, dataManager);
    }

    /**
     * 测试分页列表读取
     *
     * @throws DBException
     */
    public void test2TablePageByConnectorTree() throws DBException {
        new PageByConnectorTree(TEST_USER_OIVIEW, TEST_ACCOUNT_OIVIEW, dataManager);
    }

    /**
     * 测试获取单条记录
     *
     * @throws DBException
     */
    public void test2TableReadByConnectorTree() throws DBException {
        new ReadByConnectorTree(TEST_USER_OIVIEW, TEST_ACCOUNT_OIVIEW, dataManager);
    }

    //=======================================更新数据=======================================

    /**
     * 测试更新数据(PK)
     *
     * @throws DBException
     */
    public void testUpdateByPK() throws DBException {
        new UpdateByPK(TEST_USER_OIVIEW, getReadByPK(), dataManager);
    }

    /**
     * 测试更新数据(condition)
     *
     * @throws DBException
     */
    public void testUpdateByCondition() throws DBException {
        new UpdateByCondition(TEST_USER_OIVIEW, this.getReadByPK(), dataManager);
    }

    /**
     * 测试通过主键删除数据
     *
     * @throws DBException
     */
    public void testDeleteByPK() throws DBException {
        new DeleteByPK(TEST_USER_OIVIEW, dataManager);
    }

    /**
     * 测试通过条件删除数据
     *
     * @throws DBException
     */
    public void testDeleteByCondition() throws DBException {
        new DeleteByCondition(TEST_USER_OIVIEW, dataManager);
    }
}
