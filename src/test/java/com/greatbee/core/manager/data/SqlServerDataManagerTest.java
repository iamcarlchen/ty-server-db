package com.greatbee.core.manager.data;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.greatbee.DBBaseTest;
import com.greatbee.base.bean.DBException;
import com.greatbee.base.bean.Data;
import com.greatbee.base.bean.DataList;
import com.greatbee.base.bean.DataPage;
import com.greatbee.base.util.RandomGUIDUtil;
import com.greatbee.base.util.StringUtil;
import com.greatbee.core.bean.constant.CT;
import com.greatbee.core.bean.constant.DT;
import com.greatbee.core.bean.oi.DS;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.view.Condition;
import com.greatbee.core.bean.view.ConnectorTree;
import com.greatbee.core.bean.view.DSView;
import com.greatbee.core.bean.view.OIView;
import com.greatbee.core.manager.DSManager;
import com.greatbee.core.manager.data.sqlserver.manager.SQLServerDataManager;
import com.greatbee.core.manager.data.sqlserver.manager.SQLServerDataManagerV2;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Oracle Data Manager
 * <p/>
 * Author: CarlChen
 * Date: 2017/11/21
 */
public class SqlServerDataManagerTest extends DBBaseTest {

    @Autowired
    private DSManager dsManager;

//    @Autowired
//    private SQLServerDataManager sqlServerDataManager;
    @Autowired
    private SQLServerDataManagerV2 sqlServerDataManager;

    public void setUp() {
        super.setUp("test_server.xml");
        dsManager = (DSManager) context.getBean("dsManager");
        sqlServerDataManager = (SQLServerDataManagerV2) context.getBean("sqlServerDataManager");
    }

    /**
     * 测试导出数据源
     *
     * @return
     */
    public DSView testGetDSView() {

        try {
            DS oracleDataSource = dsManager.getDSByAlias("test_sqlserver");
            DSView dv = sqlServerDataManager.exportFromPhysicsDS(oracleDataSource);
            System.out.println("DSView -> " + JSONObject.toJSONString(dv));
            return dv;
        } catch (DBException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 取第一张表作为测试
     *
     * @return
     */
    private OIView testGetOIView() throws DBException {
        OIView oiView = null;
        DSView dsView = this.testGetDSView();
        List<OIView> oiViews = dsView.getOiViews();
        System.out.println("OIViewList -> " + JSONArray.toJSONString(oiViews));
        if (oiViews != null) {
            //选择测试用的OI
            for (OIView item : oiViews) {
                if (item.getOi().getAlias().startsWith("test")) {
                    oiView = item;
                }
            }
        }
        System.out.println("OIView -> " + JSONObject.toJSONString(oiView));
        return oiView;
    }


    /**
     * 测试插入单条数据
     *
     * @throws DBException
     */
    public void testCreateData() throws DBException {
        OIView oiView = testGetOIView();
        Field pkField = null;
        List<Field> fields = oiView.getFields();
        for (Field field : fields) {
            if (StringUtil.isValid(field.getDt()) && (field.getDt().equalsIgnoreCase(DT.Date.getType()) || field.getDt().equalsIgnoreCase(DT.Time.getType()))) {
                //插入时间类型的数据
                field.setFieldValue("2001-10-10 20:40:20");
            } else if (StringUtil.isValid(field.getDt()) && (field.getDt().equalsIgnoreCase(DT.INT.getType()) || field.getDt().equalsIgnoreCase(DT.Double.getType()))) {
                //数字类型
                field.setFieldValue("1");
            } else {
                //插入字符串类型
                field.setFieldValue(RandomGUIDUtil.getGUID(RandomGUIDUtil.RANDOM_32));
            }
        }


//        pkField.setFieldValue("2");//设置主键值
        String result = sqlServerDataManager.create(oiView.getOi(), fields);
        System.out.println("Data -> " + result);
    }


    /**
     * 测试获取列表
     *
     * @throws DBException
     */
    public void testListByConnectorTree() throws DBException {
        OIView oiView = testGetOIView();
        Field pkField = null;
        Map<String, Field> queryField = new HashMap<String, Field>();
        List<Field> fields = oiView.getFields();
        for (Field field : fields) {
            queryField.put(field.getFieldName(), field);
        }

        ConnectorTree queryTree = new ConnectorTree();
        queryTree.setOi(oiView.getOi());
        queryTree.setFields(queryField);

        DataList dataList = sqlServerDataManager.list(queryTree);
        System.out.println("Data -> " + JSONObject.toJSONString(dataList));
    }


    /**
     * 测试获取列表(condition)
     *
     * @throws DBException
     */
    public void testListByCondition() throws DBException {
        OIView oiView = testGetOIView();

        List<Field> fields = oiView.getFields();
        Condition queryCondition = new Condition();
        queryCondition.setConditionFieldName("alias");
        queryCondition.setConditionFieldValue("abc");
        queryCondition.setCt(CT.EQ.getName());

        DataList dataList = sqlServerDataManager.list(oiView.getOi(), fields, queryCondition);
        System.out.println("Data -> " + JSONObject.toJSONString(dataList));
    }


    /**
     * 测试count函数
     *
     * @throws DBException
     */
    public void testCountByConnectorTree() throws DBException {
        OIView oiView = testGetOIView();
        Field pkField = null;
        Map<String, Field> queryField = new HashMap<String, Field>();
        List<Field> fields = oiView.getFields();
        for (Field field : fields) {
            queryField.put(field.getFieldName(), field);
        }

        ConnectorTree queryTree = new ConnectorTree();
        queryTree.setOi(oiView.getOi());
        queryTree.setFields(queryField);
        int result = sqlServerDataManager.count(queryTree);
        System.out.println("count -> " + result);
    }

    /**
     * 测试分页列表读取
     *
     * @throws DBException
     */
    public void testPageByConnectorTree() throws DBException {
        OIView oiView = testGetOIView();
        Field pkField = null;
        Map<String, Field> queryField = new HashMap<String, Field>();
        List<Field> fields = oiView.getFields();
        for (Field field : fields) {
            queryField.put(field.getFieldName(), field);
        }

        ConnectorTree queryTree = new ConnectorTree();
        queryTree.setOi(oiView.getOi());
        queryTree.setFields(queryField);

        DataPage dataPage = sqlServerDataManager.page(1, 10, queryTree);
        System.out.println("Data -> " + JSONObject.toJSONString(dataPage));
    }

    /**
     * 测试分页列表读取(Condition)
     *
     * @throws DBException
     */
    public void testPageByCondition() throws DBException {
        OIView oiView = testGetOIView();

        List<Field> fields = oiView.getFields();
        Condition queryCondition = new Condition();
        queryCondition.setConditionFieldName("alias");
        queryCondition.setConditionFieldValue("abc");
        queryCondition.setCt(CT.EQ.getName());

        DataPage dataPage = sqlServerDataManager.page(oiView.getOi(), fields, 1, 10, queryCondition);
        System.out.println("Data -> " + JSONObject.toJSONString(dataPage));
    }


    /**
     * 测试获取单条记录
     *
     * @throws DBException
     */
    public void testReadByConnectorTree() throws DBException {
        OIView oiView = testGetOIView();
        Field pkField = null;
        Map<String, Field> queryField = new HashMap<String, Field>();
        List<Field> fields = oiView.getFields();
        for (Field field : fields) {
            queryField.put(field.getFieldName(), field);
        }

        ConnectorTree queryTree = new ConnectorTree();
        queryTree.setOi(oiView.getOi());
        queryTree.setFields(queryField);

        Data data = sqlServerDataManager.read(queryTree);
        System.out.println("Data -> " + JSONObject.toJSONString(data));
    }

    /**
     * 测试获取单条记录
     *
     * @throws DBException
     */
    public void testReadByPK() throws DBException {
        OIView oiView = testGetOIView();
        Field pkField = null;
        List<Field> fields = oiView.getFields();
        for (Field field : fields) {
            if (field.isPk()) {
                pkField = field;
                break;
            }
        }
        pkField.setFieldValue("1");//设置主键值
        Data data = sqlServerDataManager.read(oiView.getOi(), fields, pkField);
        System.out.println("Data -> " + JSONObject.toJSONString(data));
    }

    /**
     * 测试获取单条记录
     *
     * @throws DBException
     */
    public Data getReadByPK() throws DBException {
        OIView oiView = testGetOIView();
        Field pkField = null;
        List<Field> fields = oiView.getFields();
        for (Field field : fields) {
            if (field.isPk()) {
                pkField = field;
                break;
            }
        }
        pkField.setFieldValue("1");//设置主键值
        Data data = sqlServerDataManager.read(oiView.getOi(), fields, pkField);
        System.out.println("Data -> " + JSONObject.toJSONString(data));
        return data;
    }


    /**
     * 测试更新数据(PK)
     *
     * @throws DBException
     */
    public void testUpdateByPK() throws DBException {
        OIView oiView = testGetOIView();
        Field pkField = null;
        List<Field> fields = oiView.getFields();
        List<Field> updateFields = new ArrayList<Field>();

        Data data = this.getReadByPK();

        for (Field field : fields) {
            if (field.isPk()) {
                pkField = field;
                pkField.setFieldValue("1");
            } else {
                if (data.containsKey(field.getFieldName())) {
                    field.setFieldValue(data.getString(field.getFieldName()));
                    updateFields.add(field);
                }
            }
        }

        sqlServerDataManager.update(oiView.getOi(), updateFields, pkField);
        System.out.println("update success!");
    }

    /**
     * 测试更新数据(condition)
     *
     * @throws DBException
     */
    public void testUpdateByCondition() throws DBException {
        OIView oiView = testGetOIView();

        List<Field> fields = oiView.getFields();
        List<Field> updateFields = new ArrayList<Field>();

        Data data = this.getReadByPK();

        for (Field field : fields) {
            if (field.isPk()) {

            } else {
                if (data.containsKey(field.getFieldName())) {
                    field.setFieldValue(data.getString(field.getFieldName()));
                    updateFields.add(field);
                }
            }
        }

        Condition queryCondition = new Condition();
        queryCondition.setConditionFieldName("alias");
        queryCondition.setConditionFieldValue("abc");
        queryCondition.setCt(CT.EQ.getName());

        sqlServerDataManager.update(oiView.getOi(), updateFields, queryCondition);
        System.out.println("update success!");
    }


    /**
     * 测试通过主键删除数据
     *
     * @throws DBException
     */
    public void testDeleteByPK() throws DBException {
        OIView oiView = testGetOIView();
        Field pkField = null;
        List<Field> fields = oiView.getFields();
        for (Field field : fields) {
            if (field.isPk()) {
                pkField = field;
                break;
            }
        }
        pkField.setFieldValue("3");//设置主键值
        sqlServerDataManager.delete(oiView.getOi(), pkField);

    }


    /**
     * 测试通过条件删除数据
     *
     * @throws DBException
     */
    public void testDeleteByCondition() throws DBException {
        OIView oiView = testGetOIView();
        Field pkField = null;
        List<Field> fields = oiView.getFields();
        for (Field field : fields) {
            if (field.isPk()) {
                pkField = field;
                break;
            }
        }

        Condition deleteCondition = new Condition();
        deleteCondition.setConditionFieldName("alias");
        deleteCondition.setConditionFieldValue("abc");
        deleteCondition.setCt(CT.EQ.getName());
        sqlServerDataManager.delete(oiView.getOi(), deleteCondition);
    }


}
