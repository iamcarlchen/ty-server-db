package com.greatbee.core.manager.data.base.manager.handler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by usagizhang on 17/12/19.
 */
public interface QueryHandler {

    public PreparedStatement execute(Connection conn, PreparedStatement ps) throws SQLException;

}
