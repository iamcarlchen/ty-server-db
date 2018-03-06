package com.greatbee.core.db.mysql;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.greatbee.DBBaseTest;
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
import com.greatbee.core.db.mysql.manager.MysqlDataManager;
import com.greatbee.core.manager.DSManager;

import java.util.*;

/**
 * Created by CarlChen on 2017/5/25.
 */
public class MysqlDataManagerTestV2 extends DBBaseTest {
    private MysqlDataManager mysqlDataManager;
    private DSManager dsManager;

    private DSView dsView;

    private OIView oiView1;
    private OIView oiView2;

    private ConnectorTree connectorTree;

    private int id = 0;

    public void setUp() {
        super.setUp("test_server.xml");

        mysqlDataManager = (MysqlDataManager) context.getBean("mysqlDataManager");
        dsManager = (DSManager) context.getBean("dsManager");

        DS ds = new DS();
        ds.setAlias("test");
        ds.setConnectionUrl("jdbc:mysql://localhost:3306/ty?useUnicode=true&characterEncoding=utf8&autoReconnect=true&user=root&password=&p");
        ds.setConnectionUsername("root");
        ds.setConnectionPassword("");

    }

    // @Override
    // public void tearDown() throws Exception {
    //     super.tearDown();
    // }


    public void testRead() throws DBException {

        OI oi = new OI();
        oi.setResource("ly_meeting_photo_detail");
        oi.setAlias("ly_meeting_photo_detail");
        oi.setDsAlias("db_dragoneye");
        Map<String, Field> fields = new HashMap<String, Field>();
        Field f1 = new Field();
        f1.setFieldName("id");
        f1.setOiAlias(oi.getAlias());
        f1.setDt(DT.INT.getType());
        fields.put(f1.getFieldName(), f1);

        ConnectorTree connectorTree = new ConnectorTree();
        connectorTree.setOi(oi);
        connectorTree.setFields(fields);

        DataList dataList = mysqlDataManager.list(connectorTree);
        String result = JSONObject.toJSON(dataList).toString();
        System.out.println(result);
    }


}
