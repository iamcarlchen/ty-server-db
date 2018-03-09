package com.greatbee.core.lego;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.greatbee.base.bean.DBException;
import com.greatbee.base.util.StringUtil;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.bean.server.InputField;
import com.greatbee.core.db.SchemaDataManager;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component("schemaExecute")
public class SchemaExecute implements Lego, ExceptionCode {

    private static final Logger logger = Logger.getLogger(SchemaExecute.class);

    @Autowired
    private SchemaDataManager mysqlDataManager;

    /**
     * 执行
     */
    @Override
    public void execute(Input input, Output output) throws LegoException {
        //判断是单个执行还是批量执行
        InputField executeTypeField = input.getInputField("executeType");
        //获取schema执行的类型(默认单条执行)
        String executeType = StringUtil.getString(executeTypeField.getFieldValue(), "single");

        //判断是单个执行还是批量执行
        InputField executeDataField = input.getInputField("executeData");
        if (executeDataField == null) {
            throw new LegoException("缺少字段 executeData", ERROR_FIELD_VALIDATE_PARAMS_INVALID);
        }
        //获取schema执行的类型(默认单条执行)
        String executeData = StringUtil.getString(executeDataField.getFieldValue(), "{}");

        if (executeType.equalsIgnoreCase("multiple")) {
            // 批量执行
            JSONArray executeDataArrayJSON = JSONArray.parseArray(executeData);
            if (executeDataArrayJSON != null && executeDataArrayJSON.size() > 0) {
                for (int i = 0; i < executeDataArrayJSON.size(); i++) {
                    this.executeSchema(executeDataArrayJSON.getJSONObject(i));
                }
            }
        } else if (executeType.equalsIgnoreCase("single")) {
            // 单体执行
            JSONObject executeDataJSON = JSONObject.parseObject(executeData);
            this.executeSchema(executeDataJSON);
        }

    }

    /**
     * 执行schema
     */
    private void executeSchema(JSONObject executeDataJSON) throws LegoException {
        if (executeDataJSON == null) {
            //参数缺少抛出异常
        }
        String operationType = executeDataJSON.getString("operationType");
        if (StringUtil.isInvalid(operationType)) {
            //参数缺少抛出异常
        }
        switch (operationType) {

        case "createTable":
            _createTable(executeDataJSON);
            break;
        case "dropTable":
            _dropTable(executeDataJSON);
            break;
        case "addField":
            _addField(executeDataJSON);
            break;
        case "dropField":
            _dropField(executeDataJSON);
            break;
        case "updateField":
            _updateField(executeDataJSON);
            break;
        default:
            //没有的操作
            break;
        }
    }

    /**
     * 创建表操作
     */
    private void _createTable(JSONObject executeDataJSON) throws LegoException {
        if (!executeDataJSON.containsKey("createOI") || !executeDataJSON.containsKey("createFields")) {
            //json字段缺失
            throw new LegoException("JSON字段缺失", ERROR_LEGO_SCHEMA_EXECUTE_ERROR);
        }
        OI createOI = executeDataJSON.getJSONObject("createOI").toJavaObject(OI.class);
        JSONArray createOIFieldsArray = executeDataJSON.getJSONArray("createFields");
        List<Field> createOIFields = new ArrayList<Field>();
        if (createOIFieldsArray == null || createOIFieldsArray.size() < 1) {
            throw new LegoException("JSON字段缺失", ERROR_LEGO_SCHEMA_EXECUTE_ERROR);
        }
        for (int i = 0; i < createOIFieldsArray.size(); i++) {
            createOIFields.add(createOIFieldsArray.getJSONObject(i).toJavaObject(Field.class));
        }

        try {
            this.mysqlDataManager.createTable(createOI, createOIFields);
        } catch (DBException e) {
            e.printStackTrace();
            throw new LegoException("创建表" + createOI.getResource() + "失败", ERROR_LEGO_SCHEMA_EXECUTE_ERROR);
        }

    }

    /**
     * 删除表操作
     */
    private void _dropTable(JSONObject executeDataJSON) throws LegoException {
        if (!executeDataJSON.containsKey("dropOI")) {
            //json字段缺失
            throw new LegoException("JSON字段缺失", ERROR_LEGO_SCHEMA_EXECUTE_ERROR);
        }
        OI dropOI = executeDataJSON.getJSONObject("dropOI").toJavaObject(OI.class);
        try {
            this.mysqlDataManager.dropTable(dropOI);
        } catch (DBException e) {
            e.printStackTrace();
            throw new LegoException("删除表" + dropOI.getResource() + "失败", ERROR_LEGO_SCHEMA_EXECUTE_ERROR);
        }
    }

    /**
     * 添加字段操作
     */
    private void _addField(JSONObject executeDataJSON) throws LegoException {
        if (!executeDataJSON.containsKey("addOI") || !executeDataJSON.containsKey("addField")) {
            //json字段缺失
            throw new LegoException("JSON字段缺失", ERROR_LEGO_SCHEMA_EXECUTE_ERROR);
        }
        OI addOI = executeDataJSON.getJSONObject("addOI").toJavaObject(OI.class);
        Field addField = executeDataJSON.getJSONObject("addField").toJavaObject(Field.class);
        try {
            this.mysqlDataManager.addField(addOI, addField);
        } catch (DBException e) {
            e.printStackTrace();
            throw new LegoException("添加字段" + addOI.getResource() + "." + addField.getFieldName() + "失败",
                    ERROR_LEGO_SCHEMA_EXECUTE_ERROR);
        }
    }

    /**
     * 删除字段操作
     */
    private void _dropField(JSONObject executeDataJSON) throws LegoException {
        if (!executeDataJSON.containsKey("dropOI") || !executeDataJSON.containsKey("dropField")) {
            //json字段缺失
            throw new LegoException("JSON字段缺失", ERROR_LEGO_SCHEMA_EXECUTE_ERROR);
        }
        OI dropOI = executeDataJSON.getJSONObject("dropOI").toJavaObject(OI.class);
        Field dropField = executeDataJSON.getJSONObject("dropField").toJavaObject(Field.class);
        try {
            this.mysqlDataManager.dropField(dropOI, dropField);
        } catch (DBException e) {
            e.printStackTrace();
            throw new LegoException("删除字段" + dropOI.getResource() + "." + dropField.getFieldName() + "失败",
                    ERROR_LEGO_SCHEMA_EXECUTE_ERROR);
        }
    }

    /**
     * 更新字段操作
     */
    private void _updateField(JSONObject executeDataJSON) throws LegoException {
        if (!executeDataJSON.containsKey("updateOI") || !executeDataJSON.containsKey("updateField")) {
            //json字段缺失
            throw new LegoException("JSON字段缺失", ERROR_LEGO_SCHEMA_EXECUTE_ERROR);
        }
        OI updateOI = executeDataJSON.getJSONObject("updateOI").toJavaObject(OI.class);
        Field updateField = executeDataJSON.getJSONObject("updateField").toJavaObject(Field.class);
        try {
            this.mysqlDataManager.updateField(updateOI, updateField);
        } catch (DBException e) {
            e.printStackTrace();
            throw new LegoException("更新字段" + updateOI.getResource() + "." + updateField.getFieldName() + "失败",
                    ERROR_LEGO_SCHEMA_EXECUTE_ERROR);
        }
    }
}
