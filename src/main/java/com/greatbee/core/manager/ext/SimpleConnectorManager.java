package com.greatbee.core.manager.ext;

import com.greatbee.base.bean.DBException;
import com.greatbee.base.manager.ext.AbstractBasicManager;
import com.greatbee.base.util.CollectionUtil;
import com.greatbee.core.bean.oi.Connector;
import com.greatbee.core.manager.ConnectorManager;

import java.util.List;

/**
 * Author :CarlChen
 * Date:17/7/24
 */
public class SimpleConnectorManager  extends AbstractBasicManager implements ConnectorManager {
    public SimpleConnectorManager() {
        super(Connector.class);
    }

    @Override
    public Connector getConnectorByAlias(String alias) throws DBException {
        List<Connector> cs = this.list("alias",alias);

        return CollectionUtil.isValid(cs)?cs.get(0):null;
    }

    @Override
    public List<Connector> getConnectorByFromOiAlias(String fromOiAlias) throws DBException {
        List<Connector> cs = this.list("fromOIAlias",fromOiAlias);
        return cs;
    }
}
