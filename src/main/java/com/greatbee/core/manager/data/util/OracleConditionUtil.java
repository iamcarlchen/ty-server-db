package com.greatbee.core.manager.data.util;

import com.greatbee.base.util.BooleanUtil;
import com.greatbee.base.util.StringUtil;
import com.greatbee.core.bean.constant.CG;
import com.greatbee.core.bean.constant.CT;
import com.greatbee.core.bean.view.Condition;
import org.hibernate.Criteria;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Junction;
import org.hibernate.criterion.Restrictions;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by usagizhang on 17/12/13.
 */
public class OracleConditionUtil {

    public static void buildConditionSql(StringBuilder sql, Condition condition) {
        buildConditionSql(sql, condition, (String) null);
    }

    public static void buildConditionSql(StringBuilder sql, Condition condition, String tableAlias) {
        List conditions = condition.getConditions();
        if (conditions != null) {
            sql.append(" ( ");

            for (int i = 0; i < conditions.size(); ++i) {
                Condition contidionItem = (Condition) conditions.get(i);
                buildConditionSql(sql, contidionItem, tableAlias);
                if (i < conditions.size() - 1) {
                    sql.append(condition.getCg().equals(CG.AND) ? " AND " : " OR ");
                }
            }

            sql.append(" ) ");
        } else {
            if (StringUtil.isValid(tableAlias)) {
                sql.append(" \"").append(tableAlias).append("\".");
            }

            if (!CT.IN.getName().equalsIgnoreCase(condition.getCt()) && !CT.NOTIN.getName().equalsIgnoreCase(condition.getCt())) {
                sql.append("\"").append(condition.getConditionFieldName()).append("\" ").append(CT.getSqlType(condition.getCt())).append(" ");
                if (CT.isNeedFieldValue(condition.getCt())) {
                    sql.append(" ? ");
                }
            } else {
                sql.append("\"").append(condition.getConditionFieldName()).append("\" ").append(CT.getSqlType(condition.getCt())).append(" (");
                if (StringUtil.isInvalid(condition.getConditionFieldValue())) {
                    sql.append(" ? ");
                } else {
                    String[] var6 = condition.getConditionFieldValue().split(",");

                    for (int var7 = 0; var7 < var6.length; ++var7) {
                        if (var7 != 0) {
                            sql.append(",");
                        }

                        sql.append(" ? ");
                    }
                }

                sql.append(") ");
            }
        }

    }

    public static int buildConditionSqlPs(int index, PreparedStatement ps, Condition condition) throws SQLException {
        System.out.println("index=" + index);
        List conditions = condition.getConditions();
        int _index = index;
        if (conditions != null) {
            for (int vals = 0; vals < conditions.size(); ++vals) {
                Condition i = (Condition) conditions.get(vals);
                _index = buildConditionSqlPs(_index, ps, i);
            }
        } else if (CT.isNeedFieldValue(condition.getCt())) {
            if (CT.LeftLIKE.getName().equals(condition.getCt())) {
                _index = index + 1;
                ps.setString(index, "%" + condition.getConditionFieldValue());
            } else if (CT.RightLIKE.getName().equals(condition.getCt())) {
                _index = index + 1;
                ps.setString(index, condition.getConditionFieldValue() + "%");
            } else if (CT.LIKE.getName().equals(condition.getCt())) {
                _index = index + 1;
                ps.setString(index, "%" + condition.getConditionFieldValue() + "%");
            } else if (!CT.IN.getName().equalsIgnoreCase(condition.getCt()) && !CT.NOTIN.getName().equalsIgnoreCase(condition.getCt())) {
                _index = index + 1;
                ps.setString(index, condition.getConditionFieldValue());
            } else if (StringUtil.isInvalid(condition.getConditionFieldValue())) {
                _index = index + 1;
                ps.setString(index, "9999999999");
            } else {
                String[] var7 = condition.getConditionFieldValue().split(",");

                for (int var8 = 0; var8 < var7.length; ++var8) {
                    ps.setString(_index++, var7[var8]);
                }
            }
        }

        return _index;
    }

    public static void buildCriteriaCondition(Class beanClass, Criteria c, Junction junction, Condition condition) {
        if (condition != null) {
            List conditions = condition.getConditions();
            if (conditions != null) {
                Object junc = null;
                if (CG.AND.equals(condition.getCg())) {
                    junc = Restrictions.conjunction();
                } else {
                    junc = Restrictions.disjunction();
                }

                for (int i = 0; i < conditions.size(); ++i) {
                    Condition _condition = (Condition) conditions.get(i);
                    buildCriteriaCondition(beanClass, c, (Junction) junc, _condition);
                }

                if (junction == null) {
                    c.add((Criterion) junc);
                } else {
                    junction.add((Criterion) junc);
                }
            } else if (junction == null) {
                c.add(_transferJunction(beanClass, condition));
            } else {
                junction.add(_transferJunction(beanClass, condition));
            }
        }

    }

    private static Criterion _transferJunction(Class beanClass, Condition condition) {
        return (Criterion) (CT.EQ.getName().equals(condition.getCt()) ? Restrictions.eq(condition.getConditionFieldName(), _getConditionValue(beanClass, condition)) : (CT.GT.getName().equals(condition.getCt()) ? Restrictions.gt(condition.getConditionFieldName(), _getConditionValue(beanClass, condition)) : (CT.GE.getName().equals(condition.getCt()) ? Restrictions.ge(condition.getConditionFieldName(), _getConditionValue(beanClass, condition)) : (CT.LT.getName().equals(condition.getCt()) ? Restrictions.lt(condition.getConditionFieldName(), _getConditionValue(beanClass, condition)) : (CT.LE.getName().equals(condition.getCt()) ? Restrictions.le(condition.getConditionFieldName(), _getConditionValue(beanClass, condition)) : (CT.NEQ.getName().equals(condition.getCt()) ? Restrictions.ne(condition.getConditionFieldName(), _getConditionValue(beanClass, condition)) : (CT.IN.getName().equals(condition.getCt()) ? Restrictions.in(condition.getConditionFieldName(), condition.getConditionFieldValue().split(",")) : (CT.NOTIN.getName().equals(condition.getCt()) ? Restrictions.not(Restrictions.in(condition.getConditionFieldName(), condition.getConditionFieldValue().split(","))) : (CT.LIKE.getName().equals(condition.getCt()) ? Restrictions.like(condition.getConditionFieldName(), "%" + condition.getConditionFieldValue() + "%") : (CT.LeftLIKE.getName().equals(condition.getCt()) ? Restrictions.like(condition.getConditionFieldName(), "%" + condition.getConditionFieldValue()) : (CT.RightLIKE.getName().equals(condition.getCt()) ? Restrictions.like(condition.getConditionFieldName(), condition.getConditionFieldValue() + "%") : (CT.NULL.getName().equals(condition.getCt()) ? Restrictions.isNull(condition.getConditionFieldName()) : (CT.ISNOT.getName().equals(condition.getCt()) ? Restrictions.isNotNull(condition.getConditionFieldName()) : Restrictions.eq(condition.getConditionFieldName(), _getConditionValue(beanClass, condition)))))))))))))));
    }

    private static Object _getConditionValue(Class beanClass, Condition condition) {
        Field[] fs = beanClass.getDeclaredFields();
        int i = 0;

        while (true) {
            if (i < fs.length) {
                Field f = fs[i];
                String name = f.getName();
                Class type = f.getType();
                if (!StringUtil.isValid(condition.getConditionFieldName()) || !name.equals(condition.getConditionFieldName())) {
                    ++i;
                    continue;
                }

                if (Boolean.class.equals(type) || Boolean.TYPE.equals(type)) {
                    return Boolean.valueOf(BooleanUtil.toBool(condition.getConditionFieldValue()));
                }
            }

            return condition.getConditionFieldValue();
        }
    }
}
