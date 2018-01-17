package com.greatbee.core.bean.view;

import com.greatbee.core.bean.oi.DS;

import java.util.List;

/**
 * DS View
 * <p/>
 * Created by CarlChen on 2017/5/25.
 */
public class DSView {
    private DS ds;

    private List<OIView> oiViews;

    public DS getDs() {
        return ds;
    }

    public void setDs(DS ds) {
        this.ds = ds;
    }

    public List<OIView> getOiViews() {
        return oiViews;
    }

    public void setOiViews(List<OIView> oiViews) {
        this.oiViews = oiViews;
    }
}
