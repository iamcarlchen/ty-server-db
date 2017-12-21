package com.greatbee.core.manager.data;

import com.greatbee.DBBaseTest;
import com.greatbee.base.bean.DBException;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.manager.DSManager;
import com.greatbee.core.manager.data.sqlserver.manager.SQLServerDataManagerV2;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Created by usagizhang on 17/12/21.
 */
public abstract class DataManagerTest extends DBBaseTest implements ExceptionCode {

    @Autowired
    protected DSManager dsManager;

    @Autowired
    private RelationalDataManager dataManager;

    public DataManagerTest() {

    }

    public void setUp() throws DBException {
        super.setUp("test_server.xml");
        dsManager = (DSManager) context.getBean("dsManager");
        dataManager = this.getDataManager();
        this.initSchema();
    }

    public abstract void initSchema();

    public abstract RelationalDataManager getDataManager();



}
