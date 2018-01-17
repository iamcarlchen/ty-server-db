package com.greatbee.core.manager.data.mysql;

import com.alibaba.fastjson.JSON;
import com.greatbee.TYBaseTest;
import com.greatbee.base.bean.DBException;
import com.greatbee.base.bean.Data;
import com.greatbee.base.bean.DataList;
import com.greatbee.base.bean.DataPage;
import com.greatbee.base.util.DataUtil;
import com.greatbee.core.bean.constant.*;
import com.greatbee.core.bean.oi.Connector;
import com.greatbee.core.bean.oi.DS;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.bean.view.*;
import com.greatbee.core.db.mysql.MysqlDataManager;
import com.greatbee.core.manager.DSManager;

import java.util.*;

/**
 * Created by CarlChen on 2017/5/25.
 */
public class MysqlDataManagerTest extends TYBaseTest {
    private MysqlDataManager mysqlDataManager;
    private DSManager dsManager;

    private DSView dsView;

    private OIView oiView1;
    private OIView oiView2;

    private ConnectorTree connectorTree;

    private int id = 0;

    public void setUp() {
        super.setUp();

        mysqlDataManager = (MysqlDataManager) context.getBean("mysqlDataManager");
        dsManager = (DSManager) context.getBean("dsManager");

        DS ds = new DS();
        ds.setAlias("test");
        ds.setConnectionUrl("jdbc:mysql://localhost:3306/ty?useUnicode=true&characterEncoding=utf8&autoReconnect=true&user=root&password=&p");
        ds.setConnectionUsername("root");
        ds.setConnectionPassword("");

        dsView = new DSView();
        dsView.setDs(ds);
        List<OIView> oiViews = new ArrayList<OIView>();

        //T1 表   f1,f2,f3
        oiView1 = new OIView();
        OI oi = new OI();
        oi.setAlias("t1");
        oi.setDsAlias(ds.getAlias());
        oi.setResource("tb_t1");
        oiView1.setOi(oi);
        List<Field> fields = new ArrayList<Field>();
        Field _f = new Field();
        _f.setFieldName("f1");
        _f.setFieldLength(11);
        _f.setPk(true);
        _f.setOiAlias(oi.getAlias());
        _f.setDt(DT.INT.getType());
        fields.add(_f);
        Field _f2 = new Field();
        _f2.setDt(DT.String.getType());
        _f2.setOiAlias(oi.getAlias());
        _f2.setFieldName("f2");
        _f2.setFieldLength(255);
        fields.add(_f2);
        Field _f3 = new Field();
        _f3.setDt(DT.INT.getType());
        _f3.setOiAlias(oi.getAlias());
        _f3.setFieldName("f3");
        _f3.setFieldLength(11);
        fields.add(_f3);
        oiView1.setFields(fields);
        oiViews.add(oiView1);

        //T2 表   f4,f5,f6
        oiView2 = new OIView();
        OI oi2 = new OI();
        oi2.setAlias("t2");
        oi2.setDsAlias(ds.getAlias());
        oi2.setResource("tb_t2");
        oiView2.setOi(oi2);
        List<Field> fields2 = new ArrayList<Field>();
        Field _f4 = new Field();
        _f4.setFieldName("f4");
        _f4.setFieldLength(11);
        _f4.setPk(true);
        _f4.setOiAlias(oi.getAlias());
        _f4.setDt(DT.INT.getType());
        fields2.add(_f4);
        Field _f5 = new Field();
        _f5.setDt(DT.INT.getType());
        _f5.setOiAlias(oi.getAlias());
        _f5.setFieldName("f5");
        _f5.setFieldLength(11);
        fields2.add(_f5);
        Field _f6 = new Field();
        _f6.setDt(DT.String.getType());
        _f6.setOiAlias(oi.getAlias());
        _f6.setFieldName("f6");
        _f6.setFieldLength(255);
        fields2.add(_f6);
        oiView2.setFields(fields2);
        oiViews.add(oiView2);

        dsView.setOiViews(oiViews);

        try {
            mysqlDataManager.importFromDS(dsView);
        } catch (DBException e) {
            e.printStackTrace();
        }

        //T1 表插入几条数据
        List<Field> fs = oiView1.getFields();
        for (int i = 1; i < 10; i++) {
            for (Field ff : fs) {
                if (!ff.isPk()) {
                    ff.setFieldValue("" + new Random().nextInt(20));
                }
            }
            try {
                mysqlDataManager.create(oiView1.getOi(), fs);
            } catch (DBException e) {
                e.printStackTrace();
            }
        }

        //T2表插入几条数据
        List<Field> fs2 = oiView2.getFields();
        for (int i = 1; i < 10; i++) {
            for (Field ff : fs2) {
                if (!ff.isPk()) {
                    ff.setFieldValue("" + new Random().nextInt(20));
                }
            }
            try {
                mysqlDataManager.create(oiView2.getOi(), fs2);
            } catch (DBException e) {
                e.printStackTrace();
            }
        }


        // -------- build connectorTree  start ---------
        //构造connectorTree
        connectorTree = new ConnectorTree();

        Map<String, Field> t1Map = new HashMap<String, Field>();
        connectorTree.setOi(oiView1.getOi());
        t1Map.put(oiView1.getFields().get(1).getFieldName() + "_001", oiView1.getFields().get(1));
        connectorTree.setFields(t1Map);
        List<ConnectorTree> rootChild = new ArrayList<ConnectorTree>();
        connectorTree.setChildren(rootChild);

        ConnectorTree c1 = new ConnectorTree();
        c1.setOi(oiView2.getOi());
        Map<String, Field> t2Map = new HashMap<String, Field>();
        t2Map.put(oiView2.getFields().get(2).getFieldName() + "_002", oiView2.getFields().get(2));
        c1.setFields(t2Map);
        Connector conn = new Connector();
        conn.setAlias("c1");
        conn.setFromFieldName(_f.getFieldName());
        conn.setToFieldName(_f5.getFieldName());
        c1.setConnector(conn);

        List<ConnectorTree> c1Child = new ArrayList<ConnectorTree>();
        c1.setChildren(c1Child);
        rootChild.add(c1);

        ConnectorTree c2 = new ConnectorTree();
        c2.setConT(ConT.Right);
        c2.setOi(oiView2.getOi());
        Map<String, Field> t2Map2 = new HashMap<String, Field>();
        t2Map2.put(oiView2.getFields().get(0).getFieldName() + "_003", oiView2.getFields().get(0));
        c2.setFields(t2Map2);
        Connector conn2 = new Connector();
        conn2.setAlias("c2");
        conn2.setFromFieldName(_f2.getFieldName());
        conn2.setToFieldName(_f6.getFieldName());
        c2.setConnector(conn2);

        rootChild.add(c2);

        ConnectorTree c3 = new ConnectorTree();
        c3.setOi(oiView1.getOi());
        Map<String, Field> t1Map2 = new HashMap<String, Field>();
        t1Map2.put(oiView1.getFields().get(2).getFieldName() + "_004", oiView1.getFields().get(2));
        c3.setFields(t1Map2);
        Connector conn3 = new Connector();
        conn3.setAlias("c1");
        conn3.setFromFieldName(_f4.getFieldName());
        conn3.setToFieldName(_f3.getFieldName());
        c3.setConnector(conn3);

        c1Child.add(c3);

        //build Condition------------
        // f2=2
        MultiCondition rootCondition = new MultiCondition();
        rootCondition.setConditionFieldName(_f2.getFieldName());
        rootCondition.setConditionFieldValue("2");
        connectorTree.setCondition(rootCondition);

        //f4=1 or f5=2
        List<Condition> conditionsC1 = new ArrayList<Condition>();
        MultiCondition c1Condition = new MultiCondition(conditionsC1);
        c1Condition.setCg(CG.OR);
        Condition cc1 = new Condition();
        cc1.setConditionFieldName(_f4.getFieldName());
        cc1.setConditionFieldValue("1");
        conditionsC1.add(cc1);
//        Condition cc2 = new Condition();
//        cc2.setConditionFieldName(_f5.getFieldName());
//        cc2.setConditionFieldValue("2");
//        conditionsC1.add(cc2);
        //设置c1的条件
        c1.setCondition(c1Condition);

        //build Order By------------
        OrderBy ob = new OrderBy();
        ob.setOrder(Order.DESC);
        ob.setOrderFieldName(_f6.getFieldName());
        c1.setOrderBy(ob);

//        GroupBy gb = new GroupBy();
//        gb.setGroupFieldName(_f2.getFieldName());
//        connectorTree.setGroupBy(gb);


        // -------- build connectorTree  end ---------


    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (dsView != null) {
            mysqlDataManager.dropTable(dsView);
            dsView = null;
            oiView2 = null;
        }
    }

    /**
     * Test Export From DS
     */
    public void testExportFromPhysicsDS() throws DBException {
        DS ds = new DS();
        ds.setConnectionUrl("jdbc:mysql://db201.dev.rs.com:3306/xiwa_nvwa_818?useUnicode=true&characterEncoding=utf8");
        ds.setConnectionUsername("nvwa");
        ds.setConnectionPassword("nvwa_user");

        DSView dsView = mysqlDataManager.exportFromPhysicsDS(ds);

        System.out.println(JSON.toJSONString(dsView));
    }

    /**
     * dsView导入到数据库
     */
    public void testImportFromDS() {

        //上面已经测试过了
    }

    /**
     * 测试工具类
     *
     * @return
     */
    private DSView getDSView() {
        DS ds = new DS();
        ds.setConnectionUrl("jdbc:mysql://localhost:3306/ty?useUnicode=true&characterEncoding=utf8&autoReconnect=true&user=root&password=&p");
        ds.setConnectionUsername("root");
        ds.setConnectionPassword("");
        try {
            return mysqlDataManager.exportFromPhysicsDS(ds);
        } catch (DBException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 取第一张表作为测试
     *
     * @return
     */
    private OIView getOIView() {
        OIView oiView = null;
        DSView dsView = this.getDSView();
        List<OIView> oiViews = dsView.getOiViews();
        if (oiViews != null) {
            oiView = oiViews.get(0);
        }
        return oiView;
    }

    /**
     * 读取
     *
     * @throws DBException
     */
    public void testRead() throws DBException {
        OIView oiView = getOIView();
        Field pkField = null;
        List<Field> fields = oiView.getFields();
        for (Field field : fields) {
            if (field.isPk()) {
                pkField = field;
                break;
            }
        }
        pkField.setFieldValue("7");//设置主键值
        mysqlDataManager.read(oiView.getOi(), fields, pkField);
    }

    /**
     * 删除
     *
     * @throws DBException
     */
    public void testDelete() throws DBException {
        OIView oiView = getOIView();
        Field pkField = null;
        List<Field> fields = oiView.getFields();
        for (Field field : fields) {
            if (field.isPk()) {
                pkField = field;
                break;
            }
        }
        if (id > 0) {
            pkField.setFieldValue(String.valueOf(id));//设置主键值
        } else {
            pkField.setFieldValue("8");//设置主键值
        }
        mysqlDataManager.delete(oiView.getOi(), pkField);
    }

    /**
     * 创建
     *
     * @throws DBException
     */
    public void testCreate() throws DBException {
        OIView oiView = getOIView();
        List<Field> fields = oiView.getFields();
        for (Field field : fields) {
            if (!field.isPk()) {
                if (DT.INT.equals(field.getDt()) || DT.Double.equals(field.getDt()) || DT.Boolean.equals(field.getDt())) {
                    field.setFieldValue("0");
                } else {
                    field.setFieldValue("你好");
                }
            }
        }
        id = DataUtil.getInt(mysqlDataManager.create(oiView.getOi(), fields), 0);
    }

    /**
     * 更新
     *
     * @throws DBException
     */
    public void testUpdate() throws DBException {
        OIView oiView = getOIView();
        List<Field> fields = oiView.getFields();

        //把主键去除
        Iterator<Field> fieldsItr = fields.iterator();
        while (fieldsItr.hasNext()) {
            Field f = fieldsItr.next();
            if (f.isPk()) {
                fieldsItr.remove();
            }
        }

        Field pkField = null;
        for (Field field : fields) {
            if (!field.isPk()) {
                if (DT.INT.equals(field.getDt()) || DT.Double.equals(field.getDt()) || DT.Boolean.equals(field.getDt())) {
                    field.setFieldValue("0");
                } else {
                    field.setFieldValue("你好2");
                }
            } else {
                pkField = field;
            }
        }
        pkField.setFieldValue("6");//设置主键值   更新id为6的数据
        mysqlDataManager.update(oiView.getOi(), fields, pkField);
    }

    /**
     * 列表
     *
     * @throws DBException
     */
    public void testList() throws DBException {

        OIView oiView = getOIView();
        List<Field> fields = oiView.getFields();
        List<Condition> conList = new ArrayList<Condition>();
        for (Field field : fields) {
            if (field.getFieldName().equals("name")) {
                Condition c = new Condition();
                c.setConditionFieldName(field.getFieldName());
                c.setConditionFieldValue("你好");
                c.setCt(CT.LIKE.getName());
                conList.add(c);
            } else if (field.getFieldName().equals("alias")) {
                Condition c = new Condition();
                c.setConditionFieldName(field.getFieldName());
                c.setConditionFieldValue("你好");
                c.setCt(CT.EQ.getName());
                conList.add(c);
            }
        }
        MultiCondition condition = new MultiCondition(conList);
        mysqlDataManager.list(oiView.getOi(), fields, condition);
    }

    /**
     * 条件删除
     *
     * @throws DBException
     */
    public void testDelete2() throws DBException {
        OIView oiView = getOIView();
        List<Field> fields = oiView.getFields();
        List<Condition> conList = new ArrayList<Condition>();
        for (Field field : fields) {
            if (field.getFieldName().equals("name")) {
                Condition c = new Condition();
                c.setConditionFieldName(field.getFieldName());
                c.setConditionFieldValue("你好");
                c.setCt(CT.LIKE.getName());
                conList.add(c);
            } else if (field.getFieldName().equals("alias")) {
                Condition c = new Condition();
                c.setConditionFieldName(field.getFieldName());
                c.setConditionFieldValue("你好3");
                c.setCt(CT.EQ.getName());
                conList.add(c);
            }
        }
        MultiCondition condition = new MultiCondition(conList);
        mysqlDataManager.delete(oiView.getOi(), condition);
    }


    /**
     * 条件更新
     *
     * @throws DBException
     */
    public void testUpdate2() throws DBException {
        OIView oiView = getOIView();
        List<Field> fields = oiView.getFields();

        Iterator<Field> fieldsItr = fields.iterator();
        while (fieldsItr.hasNext()) {
            Field f = fieldsItr.next();
            if (f.isPk()) {
                fieldsItr.remove();
            }
        }

        List<Condition> conList = new ArrayList<Condition>();
        for (Field field : fields) {
            if (!field.isPk()) {
                if (DT.INT.equals(field.getDt()) || DT.Double.equals(field.getDt()) || DT.Boolean.equals(field.getDt())) {
                    field.setFieldValue("0");
                } else {
                    field.setFieldValue("你好4");
                }
            }

            if (field.getFieldName().equals("name")) {
                Condition c = new Condition();
                c.setConditionFieldName(field.getFieldName());
                c.setConditionFieldValue("你好");
                c.setCt(CT.LIKE.getName());
                conList.add(c);
            } else if (field.getFieldName().equals("alias")) {
                Condition c = new Condition();
                c.setConditionFieldName(field.getFieldName());
                c.setConditionFieldValue("你好3");
                c.setCt(CT.EQ.getName());
                conList.add(c);
            }
        }
        MultiCondition condition = new MultiCondition(conList);
        mysqlDataManager.update(oiView.getOi(), fields, condition);
    }


    /**
     * 分页查询
     *
     * @throws DBException
     */
    public void testPage() throws DBException {

        OIView oiView = getOIView();
        List<Field> fields = oiView.getFields();
        List<Condition> conList = new ArrayList<Condition>();
        for (Field field : fields) {
            if (field.getFieldName().equals("name")) {
                Condition c = new Condition();
                c.setConditionFieldName(field.getFieldName());
                c.setConditionFieldValue("你好");
                c.setCt(CT.LIKE.getName());
                conList.add(c);
            } else if (field.getFieldName().equals("alias")) {
                Condition c = new Condition();
                c.setConditionFieldName(field.getFieldName());
                c.setConditionFieldValue("你好");
                c.setCt(CT.LIKE.getName());
                conList.add(c);
            }
        }
        MultiCondition condition = new MultiCondition(conList);

        mysqlDataManager.page(oiView.getOi(), fields, 1, 2, condition);
    }

    /**
     * connectorTree形式的list查询
     *
     * @throws DBException
     */
    public void testListTree() throws DBException {

        DataList dl = mysqlDataManager.list(connectorTree);
        System.out.println("dataList=" + JSON.toJSONString(dl));

    }

    /**
     * connectorTree形式的page查询
     *
     * @throws DBException
     */
    public void testPageTree() throws DBException {

        DataPage dl = mysqlDataManager.page(1, 2, connectorTree);
        System.out.println("dataList=" + JSON.toJSONString(dl));

    }

    /**
     * connectorTree形式的read查询
     *
     * @throws DBException
     */
    public void testReadTree() throws DBException {

        Data dl = mysqlDataManager.read(connectorTree);
        System.out.println("dataList=" + JSON.toJSONString(dl));

    }

}
