package com.greatbee.core.db.sqlserver.util;

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
 * Created by usagizhang on 17/12/15.
 */
public class SqlServerConditionUtil {
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

            List<Condition> conditions = condition.getConditions();
            if (conditions != null) {
                Junction junc = null;
                if (CG.AND.equals(condition.getCg())) {
                    junc = Restrictions.conjunction();
                } else {
                    junc = Restrictions.disjunction();
                }
                //muliCondition
                for (int i = 0; i < conditions.size(); i++) {
                    Condition _condition = conditions.get(i);
                    buildCriteriaCondition(beanClass, c, junc, _condition);
                }
                if (junction == null) {
                    c.add(junc);
                } else {
                    junction.add(junc);
                }
            } else {
                //Condition
                if (junction == null) {
                    c.add(_transferJunction(beanClass, condition));
                } else {
                    junction.add(_transferJunction(beanClass, condition));
                }
            }
        }

    }

    private static Criterion _transferJunction(Class beanClass, Condition condition) {
        if (CT.EQ.getName().equals(condition.getCt())) {
            return Restrictions.eq(condition.getConditionFieldName(), _getConditionValue(beanClass, condition));
        } else if (CT.GT.getName().equals(condition.getCt())) {
            return Restrictions.gt(condition.getConditionFieldName(), _getConditionValue(beanClass, condition));
        } else if (CT.GE.getName().equals(condition.getCt())) {
            return Restrictions.ge(condition.getConditionFieldName(), _getConditionValue(beanClass, condition));
        } else if (CT.LT.getName().equals(condition.getCt())) {
            return Restrictions.lt(condition.getConditionFieldName(), _getConditionValue(beanClass, condition));
        } else if (CT.LE.getName().equals(condition.getCt())) {
            return Restrictions.le(condition.getConditionFieldName(), _getConditionValue(beanClass, condition));
        } else if (CT.NEQ.getName().equals(condition.getCt())) {
            return Restrictions.ne(condition.getConditionFieldName(), _getConditionValue(beanClass, condition));
        } else if (CT.IN.getName().equals(condition.getCt())) {
            return Restrictions.in(condition.getConditionFieldName(), condition.getConditionFieldValue().split(","));
        } else if (CT.NOTIN.getName().equals(condition.getCt())) {
            return Restrictions.not(Restrictions.in(condition.getConditionFieldName(), condition.getConditionFieldValue().split(",")));
        } else if (CT.LIKE.getName().equals(condition.getCt())) {
            return Restrictions.like(condition.getConditionFieldName(), "%" + condition.getConditionFieldValue() + "%");
        } else if (CT.LeftLIKE.getName().equals(condition.getCt())) {
            return Restrictions.like(condition.getConditionFieldName(), "%" + condition.getConditionFieldValue());
        } else if (CT.RightLIKE.getName().equals(condition.getCt())) {
            return Restrictions.like(condition.getConditionFieldName(), condition.getConditionFieldValue() + "%");
        } else if (CT.NULL.getName().equals(condition.getCt())) {
            return Restrictions.isNull(condition.getConditionFieldName());
        } else if (CT.ISNOT.getName().equals(condition.getCt())) {
            return Restrictions.isNotNull(condition.getConditionFieldName());
        } else {
            return Restrictions.eq(condition.getConditionFieldName(), _getConditionValue(beanClass, condition));
        }
        //TODO is   和 isnot   没有转换
    }

    private static Object _getConditionValue(Class beanClass, Condition condition) {
        Field[] fs = beanClass.getDeclaredFields();
        for (int i = 0; i < fs.length; i++) {
            Field f = fs[i];
            String name = f.getName();
            Class type = f.getType();
            if (StringUtil.isValid(condition.getConditionFieldName()) && name.equals(condition.getConditionFieldName())) {
                if (Boolean.class.equals(type) || boolean.class.equals(type)) {
                    return BooleanUtil.toBool(condition.getConditionFieldValue());
                }
                break;
            }
        }
        return condition.getConditionFieldValue();
    }
}
