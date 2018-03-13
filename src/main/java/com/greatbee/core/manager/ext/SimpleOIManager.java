package com.greatbee.core.manager.ext;

import com.greatbee.base.bean.DBException;
import com.greatbee.base.manager.ext.AbstractBasicManager;
import com.greatbee.base.util.CollectionUtil;
import com.greatbee.core.bean.constant.CT;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.bean.view.Condition;
import com.greatbee.core.bean.view.MultiCondition;
import com.greatbee.core.manager.OIManager;

import java.util.List;

/**
 * Simple OI Manager
 * <p/>
 * Created by CarlChen on 2017/5/24.
 */
public class SimpleOIManager extends AbstractBasicManager implements OIManager {
    public SimpleOIManager() {
        super(OI.class);
    }


    @Override
    public OI getOIByAlias(String alias) throws DBException {
        List<OI> ois = (List<OI>) this.list("alias",alias);
        return CollectionUtil.isValid(ois) ? ois.get(0) : null;
    }

    @Override
    public OI getOIByResource(String dsAlias ,String resource) throws DBException {

        MultiCondition con = new MultiCondition();
        Condition con1 = new Condition();
        con1.setConditionFieldName("resource");
        con1.setConditionFieldValue(resource);
        con1.setCt(CT.EQ.getName());
        con.addCondition(con1);
        Condition con2 = new Condition();
        con2.setConditionFieldName("dsAlias");
        con2.setConditionFieldValue(dsAlias);
        con2.setCt(CT.EQ.getName());
        con.addCondition(con2);
        List<OI> ois = (List<OI>) this.list(con);

        return CollectionUtil.isValid(ois) ? ois.get(0) : null;
    }

}
