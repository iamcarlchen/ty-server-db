package com.greatbee.core.bean.view;

import com.greatbee.core.bean.constant.CG;
import com.greatbee.core.bean.view.Condition;

import java.util.ArrayList;
import java.util.List;

/**
 * Author :CarlChen
 * Date:17/7/12
 */
public class MultiCondition extends Condition {
    private List<Condition> conditions;
    private CG cg = CG.AND;


    public MultiCondition() {
    }

    public MultiCondition(List<Condition> conditions) {
        this.conditions = conditions;
    }

    @Override
    public List<Condition> getConditions() {
        return conditions;
    }

    @Override
    public CG getCg() {
        return cg;
    }

    public void setConditions(List<Condition> conditions) {
        this.conditions = conditions;
    }

    public void setCg(CG cg) {
        this.cg = cg;
    }

    /**
     * Add Condition
     *
     * @param condition
     */
    public void addCondition(Condition condition) {
        if (conditions == null) {
            conditions = new ArrayList<Condition>();
        }

        conditions.add(condition);
    }
}
