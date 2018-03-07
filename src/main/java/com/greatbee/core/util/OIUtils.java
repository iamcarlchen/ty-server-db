package com.greatbee.core.util;

import com.greatbee.base.bean.DBException;
import com.greatbee.base.util.CollectionUtil;
import com.greatbee.base.util.StringUtil;
import com.greatbee.core.ExceptionCode;
import com.greatbee.core.bean.oi.Field;
import com.greatbee.core.bean.oi.OI;
import com.greatbee.core.bean.view.OIView;

import java.util.List;


/**
 * Created by usagizhang on 18/1/22.
 */
public class OIUtils implements ExceptionCode {
    public static void isValid(OI oi) throws DBException {
        if (oi == null) {
            //无效的OI
            throw new DBException("OI数据无效", ERROR_DB_OI_INVAlID);
        } else if (StringUtil.isInvalid(oi.getDsAlias())) {
            throw new DBException("OI的DS为空", ERROR_DB_OI_DS_INVAlID);
        } else if (StringUtil.isInvalid(oi.getAlias())) {
            throw new DBException("OI的Alias为空", ERROR_DB_OI_ALIAS_INVAlID);
        }
    }

    public static void isViewValid(OIView oiView) throws DBException {
        if (oiView == null) {
            //无效的OI
            throw new DBException("OIView数据无效", ERROR_DB_OIView_INVAlID);
        }
        isValid(oiView.getOi());
    }

    /**
     * 校验OIVIWE是否存在 字段
     *
     * @param oiView    oiView
     * @param fieldName fieldName
     * @return isExist
     * @throws DBException DBException
     */
    public static boolean hasViewField(OIView oiView, String fieldName) throws DBException {
        isViewValid(oiView);
        if (StringUtil.isInvalid(fieldName)) {
            return false;
        }
        List<Field> fieldList = oiView.getFields();
        if (CollectionUtil.isInvalid(fieldList)) {
            return false;
        }
        for (Field field : fieldList) {
            if (field != null && StringUtil.isValid(field.getFieldName()) && field.getFieldName().equalsIgnoreCase(fieldName)) {
                return true;
            }
        }
        return false;
    }
}
