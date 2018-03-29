package com.greatbee.core.db.restapi.testcase;

import com.greatbee.base.bean.DBException;
import com.greatbee.base.util.StringUtil;
import com.greatbee.core.bean.constant.DT;
import com.greatbee.core.bean.constant.RestApiFieldGroupType;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.bean.view.RestApiResponse;
import com.greatbee.core.db.restapi.basecase.BaseConnectTest;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;

/**
 * test post
 * Created by usagizhang on 18/3/28.
 */
public class URMSLoginTest extends BaseConnectTest {
    public URMSLoginTest() throws DBException {
        super();
    }

    @Override
    protected void runTest() throws DBException {
        OI oi = this.initOI("test_rest_urms", "/employee/login/ad", "urms_login");
        List<Field> fields = new ArrayList<Field>();
        fields.add(initField(DT.String, 128, RestApiFieldGroupType.Post, "appId", "808f1db7"));
        fields.add(initField(DT.String, 128, RestApiFieldGroupType.Post, "appSecret", "f6e914c40c850b2c76c5001066c799424167bedc"));
        fields.add(initField(DT.String, 128, RestApiFieldGroupType.Post, "username", "xuechao.zhang"));
        fields.add(initField(DT.String, 128, RestApiFieldGroupType.Post, "password", "z75eOjnx2yeW2sFu0WBXAQ=="));
        fields.add(initField(DT.String, 128, RestApiFieldGroupType.Method, "post", "post"));
        RestApiResponse restApiResponse = (RestApiResponse) restAPIManager.connect(oi, fields);
        printJSONObject(restApiResponse);
        Assert.assertNotNull(StringUtil.getString(restApiResponse.getHeader().get("x-auth-token")));
        Assert.assertTrue(StringUtil.getString(restApiResponse.getHeader().get("x-auth-token")).length() > 0);
        this.setAuthToken(StringUtil.getString(restApiResponse.getHeader().get("x-auth-token")));
        printJSONObject("login success -> " + this.getAuthToken());

    }

    public String login() throws DBException {
        this.runTest();
        return this.getAuthToken();
    }
}
