package com.greatbee.core.lego.oi;

import com.greatbee.TYBaseTest;
import com.greatbee.base.bean.DBException;
import com.greatbee.core.bean.oi.DS;
import com.greatbee.core.bean.server.InputField;
import com.greatbee.core.lego.*;
import com.greatbee.core.manager.DSManager;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by xiaobc on 17/7/10.
 */
public class InitDBConfigTest extends TYBaseTest {

    private static final String INPUT_DS_ALIAS_NAME = "ds_alias";

    @Autowired
    private Lego initDbConfig;
    @Autowired
    private DSManager dsManager;

    private DS ds = null;

    public void setUp()  {
        super.setUp();
        initDbConfig = (Lego) context.getBean("init_db_config");
        dsManager = (DSManager) context.getBean("dsManager");

        //数据库链接根据各自pc不同做修改
        ds = new DS();
        ds.setConnectionUrl("jdbc:mysql://localhost:3306/ty?useUnicode=true&characterEncoding=utf8&autoReconnect=true&user=root&password=&p");
        ds.setConnectionUsername("root");
        ds.setConnectionPassword("");
        ds.setAlias("xiaobc_local");

        int dsId = 0;
        try {
            dsId = dsManager.add(ds);
        } catch (DBException e) {
            e.printStackTrace();
        }
        if(dsId>0){
            ds.setId(dsId);
        }
    }

    public void testExecute() throws DBException,LegoException {

        Input input = new Input(null,null);
        List<InputField> inputFields = new ArrayList<InputField>();
        InputField ifield = new InputField();
        ifield.setFieldName(INPUT_DS_ALIAS_NAME);
        ifield.setFieldValue(ds.getAlias());
        inputFields.add(ifield);
        input.setInputFields(inputFields);

        initDbConfig.execute(input,null);

    }


    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        //TODO 再删除oi和field会很麻烦，后面做
//        if(ds.getId()>0){
//            dsManager.delete(ds.getId());
//        }
    }
}