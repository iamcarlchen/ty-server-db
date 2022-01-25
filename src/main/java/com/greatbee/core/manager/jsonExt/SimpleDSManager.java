package com.greatbee.core.manager.jsonExt;

import com.alibaba.fastjson.JSONObject;
import com.greatbee.base.manager.ext.AbstractBasicManager;
import com.greatbee.base.util.Global;
import com.greatbee.base.util.JSONUtil;
import com.greatbee.base.util.StringUtil;
import com.greatbee.core.bean.oi.DS;
import com.greatbee.core.manager.DSManager;

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
    public DS getDSByAlias(String alias) {
        JSONObject dsObj = null;
        String resolveMode = Global.getInstance().getMode();
        if (StringUtil.isValid(resolveMode) && StringUtil.equals(resolveMode, "single_api_json")) {
            dsObj = Global.getInstance().getDS(alias);
        } else {
            dsObj = JSONUtil.readJsonFile(JSONUtil.Model_Path, alias);
        }
        if (dsObj == null)
            return null;
        dsObj = JSONUtil.camelJsonName(dsObj, null);
        return (DS)dsObj.toJavaObject(DS.class);
    }

    public int add(DS ds) {
        return 0;
    }

}
