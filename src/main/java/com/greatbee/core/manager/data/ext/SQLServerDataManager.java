package com.greatbee.core.manager.data.ext;

import com.greatbee.base.bean.DBException;
import com.greatbee.base.bean.Data;
import com.greatbee.base.bean.DataList;
import com.greatbee.base.bean.DataPage;
import com.greatbee.core.bean.oi.DS;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.bean.view.Condition;
import com.greatbee.core.bean.view.ConnectorTree;
import com.greatbee.core.bean.view.DSView;
import com.greatbee.core.manager.data.RelationalDataManager;

import java.util.List;

/**
 * SQL Server Data Manager
 *
 * Author: CarlChen
 * Date: 2017/11/18
 */
public class SQLServerDataManager implements RelationalDataManager {
    @Override
    public DSView exportFromPhysicsDS(DS ds) throws DBException {
        return null;
    }

    @Override
    public void importFromDS(DSView dsView) throws DBException {

    }

    @Override
    public Data read(ConnectorTree connectorTree) throws DBException {
        return null;
    }

    @Override
    public DataPage page(int page, int pageSize, ConnectorTree connectorTree) throws DBException {
        return null;
    }

    @Override
    public DataList list(ConnectorTree connectorTree) throws DBException {
        return null;
    }

    @Override
    public int count(ConnectorTree connectorTree) throws DBException {
        return 0;
    }

    @Override
    public void connect(OI oi, List<Field> list) throws DBException {

    }

    @Override
    public Data read(OI oi, List<Field> fields, Field pkField) throws DBException {
        return null;
    }

    @Override
    public DataPage page(OI oi, List<Field> fields, int page, int pageSize, Condition condition) throws DBException {
        return null;
    }

    @Override
    public DataList list(OI oi, List<Field> fields, Condition condition) throws DBException {
        return null;
    }

    @Override
    public void delete(OI oi, Field pkField) throws DBException {

    }

    @Override
    public void delete(OI oi, Condition condition) throws DBException {

    }

    @Override
    public String create(OI oi, List<Field> fields) throws DBException {
        return null;
    }

    @Override
    public void update(OI oi, List<Field> fields, Field pkField) throws DBException {

    }

    @Override
    public void update(OI oi, List<Field> fields, Condition condition) throws DBException {

    }
}
