package com.greatbee.core.manager;

import com.greatbee.base.bean.DBException;
import com.greatbee.base.manager.BasicManager;
import com.greatbee.core.bean.oi.Connector;

import java.util.List;

/**
 * Created by CarlChen on 2016/11/11.
 */
public interface ConnectorManager extends BasicManager {
    /**
     * 通过ALias获取Connector
     * @param alias
     * @return
     * @throws DBException
     */
    public Connector getConnectorByAlias(String alias) throws DBException;

    /**
     * 通过from oi alias 获取Connector list
     * @param fromOiAlias
     * @return
     * @throws DBException
     */
    public List<Connector> getConnectorByFromOiAlias(String fromOiAlias) throws DBException;
}
