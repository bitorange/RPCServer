package org.linc.RPCServer.fieldsconverter.info;

import java.util.ArrayList;
import java.util.Date;

/**
 * 用于存储一张表的信息，包括：<br/>
 * 1. 表名 <br/>
 * 2. 表别名 <br/>
 * 3. 表内包含的字段列表 <br/>
 *
 * @author ihainan
 * @version 1.0
 */
public class TableInfo {
    public static final String UNKNOWN_TABLE = "UNKNOWN_TABLE_" + new Date().toString();
    public static final String UNKNOWN_TABLE_ALIAS = "UNKNOWN_TABLE_ALIAS_" + new Date().toString();
    public static final String FINAL_TABLE = "FINAL_TABLE_" + new Date().toString();
    public static final String NULL_TABLE = "NULL_TABLE";

    /**
     * 获取表别名
     *
     * @return 表别名
     */
    public String getAliasName() {
        return aliasName;
    }

    /**
     * 获取表名
     *
     * @return 表名
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * 获取表的所有字段
     *
     * @return 表的所有字段
     */
    public ArrayList<FieldInfo> getFields() {
        return fields;
    }

    private String tableName;    // 表名
    private String aliasName;    // 表别名
    private ArrayList<FieldInfo> fields = new ArrayList<FieldInfo>();  // 表中包含的域


    @Override
    public String toString() {
        return tableName + ", " + aliasName;
    }

    /**
     * 构造函数
     *
     * @param tableName 表名
     * @param aliasName 表别名
     * @param fields    表的所有字段
     */
    public TableInfo(String tableName, String aliasName, ArrayList<FieldInfo> fields) {
        this.tableName = tableName;
        this.aliasName = aliasName;
        this.fields = fields;
    }

    /**
     * 在数据表中寻找特定名称的字段
     *
     * @param fieldNameOrAlias 需要查找的字段名或者字段别名
     * @return FieldInfo 查找得到的字段信息，找不到返回 null
     */
    public FieldInfo findField(String fieldNameOrAlias) {
        for (FieldInfo fieldInfo : fields) {
            if (fieldNameOrAlias.equals(fieldInfo.getFiledName())
                    || fieldNameOrAlias.equals(fieldInfo.getFieldAlias())) {
                return fieldInfo;
            }
        }
        return null;
    }
}
