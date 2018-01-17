package com.greatbee.core.bean.constant;

/**
 * Connector Type
 * <p/>
 * Author :CarlChen
 * Date:17/7/19
 */
public enum ConT {
    Left("left"),
    Inner("inner"),
    Right("right");
    private String type;

    ConT(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    /**
     * 获取sql 链接字符串
     * @param cont
     * @return
     */
    public static String getSqlJoinType(ConT cont){
        if(cont.equals(ConT.Inner)){
            return " JOIN ";
        }else if(cont.equals(ConT.Right)){
            return " RIGHT JOIN ";
        }else{
            return " LEFT JOIN ";
        }
    }
}
