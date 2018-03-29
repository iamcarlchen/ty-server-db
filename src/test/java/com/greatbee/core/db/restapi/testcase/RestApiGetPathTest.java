package com.greatbee.core.db.restapi.testcase;

import com.greatbee.base.bean.DBException;
import com.greatbee.core.bean.constant.DT;
import com.greatbee.core.bean.constant.RestApiFieldGroupType;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.bean.view.RestApiResponse;
import com.greatbee.core.db.restapi.basecase.BaseConnectTest;

import java.util.ArrayList;
import java.util.List;

/**
 * test post
 * Created by usagizhang on 18/3/28.
 */
public class RestApiGetPathTest extends BaseConnectTest {
    public RestApiGetPathTest() throws DBException {
        /**
         * URMS登录
         */
        this.URMSLogin();
    }

    @Override
    protected void runTest() throws DBException {
        printJSONObject("login token -> " + this.getAuthToken());
        OI oi = this.initOI("test_rest_ty", "/config/{alias}", "test_get");
        List<Field> fields = new ArrayList<Field>();
        fields.add(initField(DT.String, 128, RestApiFieldGroupType.Path, "alias", "test-rest-api-get-demo"));
        fields.add(initField(DT.String, 128, RestApiFieldGroupType.Get, "username", "王子恒"));
        fields.add(initField(DT.String, 128, RestApiFieldGroupType.Header, "x-auth-token", this.getAuthToken()));
        RestApiResponse restApiResponse = (RestApiResponse) restAPIManager.connect(oi, fields);
        printJSONObject(restApiResponse);
    }
}
