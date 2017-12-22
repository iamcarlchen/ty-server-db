package com.greatbee.core.manager.data.sqlserver.testcase;

import com.greatbee.base.bean.DBException;
import com.greatbee.base.bean.Data;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.view.OIView;
import com.greatbee.core.manager.data.RelationalDataManager;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by usagizhang on 17/12/21.
 */
public class UpdateByPK {
    public UpdateByPK(OIView oiView, Data data, RelationalDataManager dataManager) throws DBException {
        Field pkField = null;
        List<Field> fields = oiView.getFields();
        List<Field> updateFields = new ArrayList<Field>();
//        Data data = this.getReadByPK();
        for (Field field : fields) {
            if (field.isPk()) {
                pkField = field;
                pkField.setFieldValue("1");
            } else {
                if (data.containsKey(field.getFieldName())) {
                    field.setFieldValue(data.getString(field.getFieldName()));
                    updateFields.add(field);
                }
            }
        }

        dataManager.update(oiView.getOi(), updateFields, pkField);
        System.out.println("update success!");
    }
}
