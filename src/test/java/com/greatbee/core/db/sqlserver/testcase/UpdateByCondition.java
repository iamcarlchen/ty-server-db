package com.greatbee.core.db.sqlserver.testcase;

import com.greatbee.base.bean.DBException;
import com.greatbee.base.bean.Data;
import com.greatbee.core.bean.constant.CT;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.view.Condition;
import com.greatbee.core.bean.view.OIView;
import com.greatbee.core.manager.data.RelationalDataManager;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by usagizhang on 17/12/21.
 */
public class UpdateByCondition {
    public UpdateByCondition(OIView oiView, Data data, RelationalDataManager dataManager) throws DBException {
        List<Field> fields = oiView.getFields();
        List<Field> updateFields = new ArrayList<Field>();

//        Data data = this.getReadByPK();

        for (Field field : fields) {
            if (field.isPk()) {

            } else {
                if (data.containsKey(field.getFieldName())) {
                    field.setFieldValue(data.getString(field.getFieldName()));
                    updateFields.add(field);
                }
            }
        }

        Condition queryCondition = new Condition();
        queryCondition.setConditionFieldName("alias");
        queryCondition.setConditionFieldValue("abc");
        queryCondition.setCt(CT.EQ.getName());

        dataManager.update(oiView.getOi(), updateFields, queryCondition);
        System.out.println("update success!");
    }
}
