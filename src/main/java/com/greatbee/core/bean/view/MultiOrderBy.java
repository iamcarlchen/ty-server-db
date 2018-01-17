package com.greatbee.core.bean.view;

import java.util.ArrayList;
import java.util.List;

/**
 * MultiOrderBy
 *
 * @author xiaobc
 * @date 17/11/23
 */
public class MultiOrderBy extends OrderBy{

    private List<OrderBy>  orderBys;

    public List<OrderBy> getOrderBys() {
        return orderBys;
    }

    public void setOrderBys(List<OrderBy> orderBys) {
        this.orderBys = orderBys;
    }

    public void addOrderBy(OrderBy orderBy){
        if(this.orderBys==null){
            this.orderBys = new ArrayList<>();
        }
        this.orderBys.add(orderBy);
    }
}
