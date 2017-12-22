package com.greatbee.core.manager.data.sqlserver.testcase;

import com.alibaba.fastjson.JSONObject;
import com.greatbee.base.bean.DBException;
import com.greatbee.base.bean.DataList;
import com.greatbee.core.bean.constant.CT;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.view.Condition;
import com.greatbee.core.bean.view.OIView;
import com.greatbee.core.manager.data.RelationalDataManager;

import java.util.List;

/**
 * Created by usagizhang on 17/12/21.
 */
public class ListByCondition {
    public ListByCondition(OIView oiView, RelationalDataManager dataManager) throws DBException {

        List<Field> fields = oiView.getFields();
        Condition queryCondition = new Condition();
        queryCondition.setConditionFieldName("alias");
        queryCondition.setConditionFieldValue("abc");
        queryCondition.setCt(CT.EQ.getName());

        DataList dataList = dataManager.list(oiView.getOi(), fields, queryCondition);
        System.out.println("Data -> " + JSONObject.toJSONString(dataList));
    }
}
