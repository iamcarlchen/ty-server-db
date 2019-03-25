package com.greatbee.core.manager.jsonExt;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.greatbee.base.bean.DBException;
import com.greatbee.base.manager.ext.AbstractBasicManager;
import com.greatbee.base.util.CollectionUtil;
import com.greatbee.base.util.JSONUtil;
import com.greatbee.core.bean.constant.CT;
import com.greatbee.core.bean.constant.JSONSchema;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.bean.view.Condition;
import com.greatbee.core.bean.view.MultiCondition;
import com.greatbee.core.manager.OIManager;

import java.util.List;

/**
 * Simple OI Manager
 * <p/>
 * Created by Xiaobc on 2019/3/25.
 */
public class SimpleOIManager extends AbstractBasicManager implements OIManager {
    public SimpleOIManager() {
        super(OI.class);
    }


    @Override
    public OI getOIByAlias(String alias) throws DBException {
        JSONObject dsObj = JSONUtil.readJsonFile(JSONUtil.Model_Path, JSONSchema.Mokelay_DS_Alias);
        if(dsObj==null){
            return null;
        }
        JSONArray ois = dsObj.getJSONArray(JSONSchema.JSON_Field_OIS);
        for(int i=0;i<ois.size();i++){
            JSONObject oi = ois.getJSONObject(i);
            if(oi.containsKey(JSONSchema.JSON_Field_Alias) && oi.getString(JSONSchema.JSON_Field_Alias).equals(alias)){
                return oi.toJavaObject(OI.class);
            }
        }
        return null;
    }

    @Override
    public OI getOIByResource(String dsAlias ,String resource) throws DBException {
        JSONObject dsObj = JSONUtil.readJsonFile(JSONUtil.Model_Path, dsAlias);
        if(dsObj==null){
            return null;
        }
        JSONArray ois = dsObj.getJSONArray(JSONSchema.JSON_Field_OIS);
        for(int i=0;i<ois.size();i++){
            JSONObject oi = ois.getJSONObject(i);
            if(oi.containsKey(JSONSchema.JSON_Field_Resource) && oi.getString(JSONSchema.JSON_Field_Resource).equals(resource)){
                return oi.toJavaObject(OI.class);
            }
        }
        return null;
    }

}
