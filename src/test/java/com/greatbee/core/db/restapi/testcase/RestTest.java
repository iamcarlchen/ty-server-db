package com.greatbee.core.db.restapi.testcase;

import com.greatbee.base.bean.DBException;
import com.greatbee.core.bean.constant.DT;
import com.greatbee.core.bean.constant.RestApiFieldGroupType;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.bean.view.RestApiResponse;
import com.greatbee.core.db.restapi.basecase.BaseConnectTest;
import com.greatbee.core.util.SpringContextUtil;
import org.springframework.context.ApplicationContext;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * test post
 * Created by usagizhang on 18/3/28.
 */
public class RestTest extends BaseConnectTest {
    public RestTest() throws DBException {
        super();
    }

    @Override
    protected void runTest() throws DBException {
        try {
            OI oi = this.initOI("test_rest_urms", "/employee/login/ad", "urms_login");
            List<Field> fields = new ArrayList<Field>();
            fields.add(initField(DT.String, 128, RestApiFieldGroupType.Post, "appId", "808f1db7"));
            fields.add(initField(DT.String, 128, RestApiFieldGroupType.Post, "appSecret", "f6e914c40c850b2c76c5001066c799424167bedc"));
            fields.add(initField(DT.String, 128, RestApiFieldGroupType.Post, "username", "xuechao.zhang"));
            fields.add(initField(DT.String, 128, RestApiFieldGroupType.Post, "password", "z75eOjnx2yeW2sFu0WBXAQ=="));
            fields.add(initField(DT.String, 128, RestApiFieldGroupType.Method, "post", "post"));


            ApplicationContext wac = SpringContextUtil.getApplicationContext();
            Class c = Class.forName("com.greatbee.core.db.UnstructuredDataManager");
            Object restAPIManager = wac.getBean("restAPIManager");
            Method entryPoint = c.getMethod("connect", OI.class, List.class);
            RestApiResponse restApiResponse = (RestApiResponse) entryPoint.invoke(restAPIManager, oi, fields);
            printJSONObject(restApiResponse);


        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
    }
}
