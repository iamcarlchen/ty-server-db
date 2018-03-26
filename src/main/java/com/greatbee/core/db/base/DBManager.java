package com.greatbee.core.db.base;

import com.greatbee.base.bean.DBException;
import com.greatbee.base.bean.Data;
import com.greatbee.base.bean.DataList;
import com.greatbee.base.bean.DataPage;
import com.greatbee.base.util.DataUtil;
import com.greatbee.base.util.StringUtil;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.bean.constant.DT;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.manager.DSManager;
import com.greatbee.core.handler.DataHandler;
import com.greatbee.core.handler.QueryHandler;
import com.greatbee.core.util.DataSourceUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * SQL Server Data Manager
 * <p>
 * Author: xuechao.zhang
 * Date: 2017/11/18
 */
public abstract class DBManager extends BaseTYJDBCTemplate implements ExceptionCode {

    private static Logger logger = Logger.getLogger(DBManager.class);


    protected DataList executeListQuery(String dataSourceAlias, QueryHandler queryHandler, DataHandler dataHandler) throws DBException {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            conn = this.getConnection(dataSourceAlias);
            ps = queryHandler.execute(conn, ps);
            rs = ps.executeQuery();
            ArrayList list = new ArrayList();


            while (rs.next()) {
                Data item = new Data();
                dataHandler.execute(rs, item);
                list.add(item);
            }

            DataList dataList = new DataList(list);
            return dataList;
        } catch (SQLException exception) {
            exception.printStackTrace();
            throw new DBException(exception.getMessage(), ERROR_DB_SQL_EXCEPTION);
        } finally {
            this.releaseResultSet(rs);
            this.releasePreparedStatement(ps);
            this.releaseConnection(conn);
        }
    }

    protected DataPage executePageQuery(String dataSourceAlias, int page, int pageSize, int count, QueryHandler queryHandler, DataHandler dataHandler) throws DBException {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        DataPage result = new DataPage();
        try {
            conn = this.getConnection(dataSourceAlias);
            ps = queryHandler.execute(conn, ps);
            rs = ps.executeQuery();
            System.out.println("总记录数：" + count);
            ArrayList list = new ArrayList();
            while (rs.next()) {
                Data data = new Data();
                dataHandler.execute(rs, data);
                list.add(data);
            }

            DataPage dataPage = new DataPage();
            dataPage.setCurrentPage(page);
            dataPage.setCurrentRecords(list);
            dataPage.setCurrentRecordsNum(list.size());
            dataPage.setPageSize(pageSize);
            dataPage.setTotalPages(count % pageSize > 0 ? count / pageSize + 1 : count / pageSize);
            dataPage.setTotalRecords(count);
            result = dataPage;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new DBException(e.getMessage(), ERROR_DB_SQL_EXCEPTION);
        } finally {
            this.releaseResultSet(rs);
            this.releasePreparedStatement(ps);
            this.releaseConnection(conn);
        }

        return result;
    }

    protected Data executeReadQuery(String dataSourceAlias, QueryHandler queryHandler, DataHandler dataHandler) throws DBException {

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            conn = this.getConnection(dataSourceAlias);
            ps = queryHandler.execute(conn, ps);
            rs = ps.executeQuery();

            Data result = new Data();
            if (rs.next()) {
                dataHandler.execute(rs, result);
            }
            return result;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new DBException(e.getMessage(), ERROR_DB_SQL_EXCEPTION);
        } finally {
            this.releaseConnection(conn);
        }
    }

    protected int executCountQuery(String dataSourceAlias, QueryHandler handler) throws DBException {
        int result = 0;

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            conn = this.getConnection(dataSourceAlias);
            ps = handler.execute(conn, ps);
            for (rs = ps.executeQuery(); rs.next(); result = rs.getInt(1)) {
                ;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new DBException(e.getMessage(), ERROR_DB_SQL_EXCEPTION);
        } finally {
            this.releaseResultSet(rs);
            this.releasePreparedStatement(ps);
            this.releaseConnection(conn);
        }

        return result;
    }

    protected void executUpdateQuery(String dataSourceAlias, QueryHandler handler) throws DBException {
        Connection conn = null;
        PreparedStatement ps = null;

        try {
            conn = this.getConnection(dataSourceAlias);
            ps = handler.execute(conn, ps);
            if (ps != null) {
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new DBException(e.getMessage(), ERROR_DB_SQL_EXCEPTION);
        } finally {
            this.releasePreparedStatement(ps);
            this.releaseConnection(conn);
        }
    }


    protected static String _transferMysqlTypeToTySqlType(int type, int colSize) {
        return type != 4 && (type != -6 || colSize <= 4) && type != -7 && type != -5 ? (type != 8 && type != 3 ? (type == 16 || type == -6 && colSize <= 4 ? DT.Boolean.getType() : (type != 12 && type != -1 && type != -9 && type != -16 && type != 2005 && type != 1 ? (type == 91 ? DT.Date.getType() : (type != 92 && type != 93 ? (type == 2000 ? DT.Object.getType() : (type == 2003 ? DT.Array.getType() : "")) : DT.Time.getType())) : DT.String.getType())) : DT.Double.getType()) : DT.INT.getType();
    }
}
