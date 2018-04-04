package com.greatbee.core.bean.constant;

/**
 * Created by usagizhang on 18/3/26.
 */
public enum RestApiFieldGroupType {

    Header("header"), Cookie("cookie"), Post("post"), Get("get"), Path("path"), Method("method");
    private String type;

    RestApiFieldGroupType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}

//
//public enum Order {
//    ASC("ASC"), DESC("DESC");
//    private String type;
//
//    Order(String type) {
//        this.type = type;
//    }
//
//    public String getType() {
//        return type;
//    }
//
//    /**
//     * 获取Order对象
//     *
//     * @param type
//     * @return
//     */
//    public static Order getOrder(String type) {
//        Order[] orders = Order.values();
//        for (Order o : orders) {
//            if (o.getType().equalsIgnoreCase(type)) {
//                return o;
//            }
//        }
//        return Order.ASC;
//    }
//}
