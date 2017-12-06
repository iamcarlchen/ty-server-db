package com.greatbee.core.manager;

import com.alibaba.fastjson.JSONObject;
import com.greatbee.DBBaseTest;
import com.greatbee.base.bean.DBException;
import com.greatbee.core.bean.oi.DS;
import com.greatbee.core.bean.view.ConnectorTree;
import com.greatbee.core.bean.view.DSView;
import com.greatbee.core.manager.data.ext.OracleDataManager;
import org.apache.poi.util.SystemOutLogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.converter.json.GsonBuilderUtils;

/**
 * Oracle Data Manager
 * <p/>
 * Author: CarlChen
 * Date: 2017/11/21
 */
public class OracleDataManagerTest extends DBBaseTest {

    @Autowired
    private DSManager dsManager;

    @Autowired
    private OracleDataManager oracleDataManager;

    public void setUp() {
        super.setUp("test_server.xml");
        dsManager = (DSManager) context.getBean("dsManager");
        oracleDataManager = (OracleDataManager) context.getBean("oracleDataManager");
    }


//
//    /**
//     * Test Read
//     *
//     * @throws DBException
//     */
//    public void testRead() throws DBException {
//        ConnectorTree connectorTree = new ConnectorTree();
//        oracleDataManager.read(connectorTree);
//    }

    /**
     * 测试导出数据源
     */
    public void testExportFromPhysicsDS() {

        try {
            DS oracleDataSource = dsManager.getDSByAlias("test_oracle");
            DSView dv = oracleDataManager.exportFromPhysicsDS(oracleDataSource);
            System.out.println("DSView -> " + JSONObject.toJSONString(dv));
        } catch (DBException e) {
            e.printStackTrace();
        }
    }


}
