package com.greatbee.core.bean.view;

import com.greatbee.core.bean.constant.Order;

/**
 * Order By
 *
 * Author :CarlChen
 * Date:17/7/20
 */
public class OrderBy {
    private String orderFieldName;
    private Order order=Order.ASC;

    public Order getOrder() {
        return order;
    }

    public void setOrder(Order order) {
        this.order = order;
    }

    public String getOrderFieldName() {
        return orderFieldName;
    }

    public void setOrderFieldName(String orderFieldName) {
        this.orderFieldName = orderFieldName;
    }
}
