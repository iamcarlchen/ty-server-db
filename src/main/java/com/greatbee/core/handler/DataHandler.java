package com.greatbee.core.handler;

import com.greatbee.base.bean.Data;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by usagizhang on 17/12/19.
 */
public interface DataHandler {

    public void execute(ResultSet rs,Data data) throws SQLException;

}
