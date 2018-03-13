package com.greatbee.core.manager.ext;

import com.greatbee.base.bean.DBException;
import com.greatbee.base.manager.ext.AbstractBasicManager;
import com.greatbee.core.bean.constant.CT;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.view.Condition;
import com.greatbee.core.bean.view.MultiCondition;
import com.greatbee.core.manager.FieldManager;

import java.util.List;

/**
 * Simple OI Manager
 * <p/>
 * Created by CarlChen on 2017/5/24.
 */
public class SimpleFieldManager extends AbstractBasicManager implements FieldManager {
    public SimpleFieldManager() {
        super(Field.class);
    }


    @Override
    public List<Field> getFields(String oiAlias) throws DBException {
        return (List<Field>) this.list("oiAlias",oiAlias);
    }

    @Override
    public List<Field> getFields(String oiAlias, String fieldName) throws DBException {
        MultiCondition con = new MultiCondition();
        Condition con1 = new Condition();
        con1.setConditionFieldName("oiAlias");
        con1.setConditionFieldValue(oiAlias);
        con1.setCt(CT.EQ.getName());
        con.addCondition(con1);
        Condition con2 = new Condition();
        con2.setConditionFieldName("fieldName");
        con2.setConditionFieldValue(fieldName);
        con2.setCt(CT.EQ.getName());
        con.addCondition(con2);
        return (List<Field>) this.list(con);
    }
}
