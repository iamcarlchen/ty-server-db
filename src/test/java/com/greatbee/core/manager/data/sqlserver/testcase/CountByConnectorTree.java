package com.greatbee.core.manager.data.sqlserver.testcase;

import com.greatbee.base.bean.DBException;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.view.ConnectorTree;
import com.greatbee.core.bean.view.OIView;
import com.greatbee.core.manager.data.RelationalDataManager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by usagizhang on 17/12/21.
 */
public class CountByConnectorTree {
    public CountByConnectorTree(OIView oiView, RelationalDataManager dataManager) throws DBException {
        Field pkField = null;
        Map<String, Field> queryField = new HashMap<String, Field>();
        List<Field> fields = oiView.getFields();
        for (Field field : fields) {
            queryField.put(field.getFieldName(), field);
        }

        ConnectorTree queryTree = new ConnectorTree();
        queryTree.setOi(oiView.getOi());
        queryTree.setFields(queryField);
        int result = dataManager.count(queryTree);
        System.out.println("count -> " + result);
    }
}
