package com.greatbee.core.lego;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.greatbee.base.bean.DBException;
import com.greatbee.base.util.StringUtil;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.bean.oi.DS;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.bean.server.InputField;
import com.greatbee.core.db.SchemaDataManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component("restApiExecute")
public class RestApiExecute implements Lego, ExceptionCode {

    private static final Logger logger = Logger.getLogger(RestApiExecute.class);

    @Autowired
    private SchemaDataManager mysqlDataManager;


    /**
     * 执行
     */
    @Override
    public void execute(Input input, Output output) throws LegoException {
        //判断method
        InputField methodField = input.getInputField("method");
        //获取method(默认get)
        String method = StringUtil.getString(methodField.getFieldValue(), "get");
        //获取对应的oi
        //获取对应的field
        String oiAlias = input.getApiLego().getOiAlias();

        //通过 oiAlias获取OI
        OI oi;
//        try {
//            oi = tyDriver.getTyCacheService().getOIByAlias(oiAlias);
//        } catch (DBException e) {
//            e.printStackTrace();
//            logger.error(e.getMessage(), e);
//            throw new LegoException(e.getMessage(), e, e.getCode());
//        }
//
//        //通过oi.dsAlias获取DS
//        DS ds;
//        try {
//            ds = tyDriver.getTyCacheService().getDSByAlias(oi.getDsAlias());
//        } catch (DBException e) {
//            e.printStackTrace();
//            logger.error(e.getMessage(), e);
//            throw new LegoException(e.getMessage(), e, e.getCode());
//        }

        try {
            Class.forName("String");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


        output.setOutputValue("response", true);

    }


}
