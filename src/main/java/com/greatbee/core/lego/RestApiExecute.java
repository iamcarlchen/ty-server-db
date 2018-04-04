package com.greatbee.core.lego;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.greatbee.base.bean.DBException;
import com.greatbee.base.util.CollectionUtil;
import com.greatbee.base.util.StringUtil;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.bean.constant.DT;
import com.greatbee.core.bean.constant.IOFT;
import com.greatbee.core.bean.constant.RestApiFieldGroupType;
import com.greatbee.core.bean.oi.DS;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.bean.server.InputField;
import com.greatbee.core.bean.view.RestApiResponse;
import com.greatbee.core.db.SchemaDataManager;
import com.greatbee.core.db.rest.RestAPIManager;
import com.greatbee.core.manager.FieldManager;
import com.greatbee.core.manager.OIManager;
import com.greatbee.core.util.SpringContextUtil;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

@Component("restApiExecute")
public class RestApiExecute implements Lego, ExceptionCode {

    private static final Logger logger = Logger.getLogger(RestApiExecute.class);

    @Autowired
    protected RestAPIManager restAPIManager;
    @Autowired
    private OIManager oiManager;
    @Autowired
    private FieldManager fieldManager;

    /**
     * 执行
     */
    @Override
    public void execute(Input input, Output output) throws LegoException {
        ApplicationContext wac = SpringContextUtil.getApplicationContext();
        //判断method
//        InputField methodField = input.getInputField("method");
//        //获取method(默认get)
//        String method = StringUtil.getString(methodField.getFieldValue(), "get");
        //获取对应的oi
        //获取对应的field
        List<InputField> inputFields = input.getInputField(IOFT.Unstructured);
        String oiAlias = input.getApiLego().getOiAlias();

        //通过 oiAlias获取OI
        OI oi;
        List<Field> oiFields = new ArrayList<Field>();
        List<Field> restFieldsList = new ArrayList<Field>();
        RestApiResponse restApiResponse;
        try {
//            Class c = Class.forName("com.greatbee.core.manager.TyCacheService");
//            Object tyCacheService = wac.getBean("tyCacheService");
//            Method getOIByAliasMethod = c.getMethod("getOIByAlias", String.class);
//            oi = (OI) getOIByAliasMethod.invoke(tyCacheService, oiAlias);
            oi = oiManager.getOIByAlias(oiAlias);

//            Method oiAliasMethod = c.getMethod("getFields", String.class);
//            oiFields = (List<Field>) oiAliasMethod.invoke(tyCacheService, oiAlias);
            oiFields = fieldManager.getFields(oiAlias);
            if (CollectionUtil.isValid(oiFields)) {
                for (Field field : oiFields) {
                    InputField target = null;
                    for (InputField inputField : inputFields) {
                        if (inputField.getFieldName().equalsIgnoreCase(field.getFieldName())) {
                            target = inputField;
                            break;
                        }
                    }
                    if (target != null) {
                        field.setFieldValue(target.fieldValueToString());
                        restFieldsList.add(field);
                    }
                }
            }
            restApiResponse = (RestApiResponse) restAPIManager.connect(oi, restFieldsList);

        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage(), e);
            throw new LegoException(e.getMessage(), e, ERROR_LEGO_REST_EXECUTE_ERROR);
        }

//        List<Field> fields = new ArrayList<Field>();
//        fields.add(initField(DT.String, 128, RestApiFieldGroupType.Post, "appId", "808f1db7"));
//        fields.add(initField(DT.String, 128, RestApiFieldGroupType.Post, "appSecret", "f6e914c40c850b2c76c5001066c799424167bedc"));
//        fields.add(initField(DT.String, 128, RestApiFieldGroupType.Post, "username", "xuechao.zhang"));
//        fields.add(initField(DT.String, 128, RestApiFieldGroupType.Post, "password", "z75eOjnx2yeW2sFu0WBXAQ=="));
//        fields.add(initField(DT.String, 128, RestApiFieldGroupType.Method, "post", "post"));

        /**
         * 返回内容
         */
        output.setOutputValue("response", restApiResponse);
        output.setOutputValue("response_json", JSONObject.parse(restApiResponse.getResponseBody()));
    }


}
