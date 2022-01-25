package com.greatbee.core.manager.jsonExt;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.greatbee.base.bean.DBException;
import com.greatbee.base.manager.ext.AbstractBasicManager;
import com.greatbee.base.util.Global;
import com.greatbee.base.util.JSONUtil;
import com.greatbee.base.util.StringUtil;
import com.greatbee.core.bean.constant.CT;
import com.greatbee.core.bean.constant.JSONSchema;
import com.greatbee.core.bean.oi.Connector;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.view.Condition;
import com.greatbee.core.bean.view.MultiCondition;
import com.greatbee.core.manager.FieldManager;

import java.util.ArrayList;
import java.util.List;

/**
 * Simple OI Manager
 * <p/>
 * Created by Xiaobc on 2019/3/25.
 */
public class SimpleFieldManager extends AbstractBasicManager implements FieldManager {
    public SimpleFieldManager() {
        super(Field.class);
    }


    /**
     * {
     * 		ds_fields:'',
     * 		ois:[{
     * 			oi_fields:'',
     * 			fields:[{
     * 				field_fields:''
     *          }],
     * 			connector:[{	//fromOiAlias==oiAlias
     * 				connector_fields:''
     *           }]
     *       }]
     *    }
     * @param oiAlias
     * @return
     * @throws DBException
     */
    @Override
    public List<Field> getFields(String oiAlias) {
        JSONObject dsObj = null;
        String resolveMode = Global.getInstance().getMode();
        if (StringUtil.isValid(resolveMode) && StringUtil.equals(resolveMode, "single_api_json")) {
            dsObj = Global.getInstance().findDs("oi_alias", oiAlias);
        } else {
            dsObj = JSONUtil.getRootObjByAlias("oi", oiAlias);
        }
        if (dsObj == null)
            return null;
        dsObj = JSONUtil.camelJsonName(dsObj, null);
        JSONArray ois = dsObj.getJSONArray(JSONSchema.JSON_Field_OIS);
        for (int i = 0; i < ois.size(); i++) {
            JSONObject oi = ois.getJSONObject(i);
            if (oi.containsKey(JSONSchema.JSON_Field_Alias) && oi.getString(JSONSchema.JSON_Field_Alias).equals(oiAlias)) {
                JSONArray fields = oi.getJSONArray(JSONSchema.JSON_Field_Fields);
                return fields.toJavaList(Field.class);
            }
        }
        return null;
    }

    public List<Field> getFields(String oiAlias, String fieldName) throws DBException {
        List<Field> list = new ArrayList<>();
        JSONObject dsObj = null;
        String resolveMode = Global.getInstance().getMode();
        if (StringUtil.isValid(resolveMode) && StringUtil.equals(resolveMode, "single_api_json")) {
            dsObj = Global.getInstance().findDs("oi_alias", oiAlias);
        } else {
            dsObj = JSONUtil.getRootObjByAlias("oi", oiAlias);
        }
        if (dsObj == null)
            return null;
        dsObj = JSONUtil.camelJsonName(dsObj, null);
        JSONArray ois = dsObj.getJSONArray(JSONSchema.JSON_Field_OIS);
        for (int i = 0; i < ois.size(); i++) {
            JSONObject oi = ois.getJSONObject(i);
            if (oi.containsKey(JSONSchema.JSON_Field_Alias) && oi.getString(JSONSchema.JSON_Field_Alias).equals(oiAlias)) {
                JSONArray fields = oi.getJSONArray(JSONSchema.JSON_Field_Fields);
                for (int j = 0; j < fields.size(); j++) {
                    JSONObject field = fields.getJSONObject(j);
                    if (field.containsKey(JSONSchema.JSON_Field_Field_Name) && field.getString(JSONSchema.JSON_Field_Field_Name).equals(fieldName))
                        list.add(field.toJavaObject(Field.class));
                }
            }
        }
        return list;
    }

    public int add(Field field) {
        return 0;
    }
}
