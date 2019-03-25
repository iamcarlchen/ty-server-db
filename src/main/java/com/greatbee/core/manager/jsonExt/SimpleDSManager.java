package com.greatbee.core.manager.jsonExt;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.greatbee.base.bean.DBException;
import com.greatbee.base.manager.ext.AbstractBasicManager;
import com.greatbee.base.util.CollectionUtil;
import com.greatbee.base.util.JSONUtil;
import com.greatbee.core.bean.constant.JSONSchema;
import com.greatbee.core.bean.oi.DS;
import com.greatbee.core.manager.DSManager;

import java.util.List;

/**
 * Simple DS Manager
 * <p/>
 * Created by Xiaobc on 2019/3/25.
 */
public class SimpleDSManager extends AbstractBasicManager implements DSManager {
    public SimpleDSManager() {
        super(DS.class);
    }

    @Override
    public DS getDSByAlias(String alias) throws DBException {
        JSONObject dsObj = JSONUtil.readJsonFile(JSONUtil.Model_Path, JSONSchema.Mokelay_DS_Alias);
        if(dsObj==null){
            return null;
        }
        return dsObj.toJavaObject(DS.class);
    }

}
