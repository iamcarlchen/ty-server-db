package com.greatbee.core.bean.constant;

/**
 * 数据库事务类型
 */
public enum DBTT {
    Create("create"),
    Update("update"),
    Delete("delete");
    private String type;

    DBTT(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    /**
     * 通过DST获取DBMT
     *
     * @param _dbtt
     * @return
     */
    public static final DBTT getDST(String _dbtt) {
        DBTT[] arr = DBTT.values();
        for (DBTT dst : arr) {
            if (dst.getType().equalsIgnoreCase(_dbtt)) {
                return dst;
            }
        }
        return null;
    }
}
