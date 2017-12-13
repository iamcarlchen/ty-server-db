package com.greatbee.core.manager;

import com.alibaba.fastjson.JSONObject;
import com.greatbee.DBBaseTest;
import com.greatbee.base.bean.DBException;
import com.greatbee.base.bean.Data;
import com.greatbee.core.bean.oi.DS;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.view.ConnectorTree;
import com.greatbee.core.bean.view.DSView;
import com.greatbee.core.bean.view.OIView;
import com.greatbee.core.manager.data.ext.OracleDataManager;
import org.apache.poi.util.SystemOutLogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.converter.json.GsonBuilderUtils;

import java.util.List;

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

    /**
     * 测试导出数据源
     *
     * @return
     */
    public DSView getDSView() {

        try {
            DS oracleDataSource = dsManager.getDSByAlias("test_oracle");
            DSView dv = oracleDataManager.exportFromPhysicsDS(oracleDataSource);
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
    private OIView getOIView() throws DBException {
        OIView oiView = null;
        DSView dsView = this.getDSView();
        List<OIView> oiViews = dsView.getOiViews();
        if (oiViews != null) {
            //选择测试用的OI
            for (OIView item : oiViews) {
                if (item.getOi().getAlias().startsWith("test")) {
                    oiView = item;
                }
            }
        }
        return oiView;
    }


    /**
     * 测试获取单条记录
     *
     * @throws DBException
     */
    public void testReadByConnectorTree() throws DBException {
        OIView oiView = getOIView();
        Field pkField = null;
        List<Field> fields = oiView.getFields();
        for (Field field : fields) {
            if (field.isPk()) {
                pkField = field;
                break;
            }
        }
        pkField.setFieldValue("2");//设置主键值
        Data data = oracleDataManager.read(oiView.getOi(), fields, pkField);
        System.out.println("Data -> " + JSONObject.toJSONString(data));
    }


}
