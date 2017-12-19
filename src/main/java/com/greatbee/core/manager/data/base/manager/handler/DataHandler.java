package com.greatbee.core.manager.data.base.manager.handler;

import com.greatbee.base.bean.Data;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by usagizhang on 17/12/19.
 */
public interface DataHandler {

    public Data execute(ResultSet rs) throws SQLException;

}
