package com.greatbee.core.bean.constant;

import java.sql.SQLClientInfoException;

/**
 * DB Manager Type;
 * <p/>
 * Author: CarlChen
 * Date: 2018/1/18
 */
public enum DBMT {
    Mysql("mysqlDataManager"),
    Oracle("oracleDataManager"),
    SqlServer("sqlServerDataManager");
    private String type;

    DBMT(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    /**
     * 通过DST获取DBMT
     *
     * @param dst
     * @return
     */
    public static final DBMT getDBMT(String dst) {
        DST _dst = DST.getDST(dst);
        if (_dst != null) {
            if (DST.Mysql.equals(_dst)) {
                return Mysql;
            } else if (DST.Oracle.equals(_dst)) {
                return Oracle;
            } else if (DST.SqlServer.equals(_dst)) {
                return SqlServer;
            }
        }
        return null;
    }
}
