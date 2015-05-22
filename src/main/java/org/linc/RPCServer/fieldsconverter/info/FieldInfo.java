package org.linc.RPCServer.fieldsconverter.info;

import org.apache.hadoop.hive.ql.parse.ASTNode;

/**
 * 表示一个字段内包含的信息
 */
public class FieldInfo {
    /**
     * 获取字段名
     *
     * @return 字段名
     */
    public String getFiledName() {
        return filedName;
    }

    /**
     * 获取字段别名
     *
     * @return 字段别名
     */
    public String getFieldAlias() {
        return fieldAlias;
    }

    private String filedName;
    private String fieldAlias;
    private TableInfo directFromTable;

    /**
     * 字段的直接来源表
     *
     * @return 字段的直接来源表
     */
    public TableInfo getDirectFromTable() {
        return directFromTable;
    }

    /**
     * 构造函数
     *
     * @param fieldName  字段名
     * @param fieldAlias 字段别名
     */
    public FieldInfo(String fieldName, String fieldAlias, TableInfo directFromTable) {
        this.fieldAlias = fieldAlias;
        this.filedName = fieldName;
        this.directFromTable = directFromTable;
    }

    /**
     * 输出字段
     *
     * @return 字段名 + ", " + 字段别名
     */
    @Override
    public String toString() {
        return this.filedName + ", " + this.fieldAlias;
    }

    /**
     * 比较两个字段是否为同一字段
     *
     * @param fieldInfoObj 需要比较的对象
     * @return true 表示相等，false 表示不相等
     */
    @Override
    public boolean equals(Object fieldInfoObj) {
        FieldInfo fieldInfo = (FieldInfo) fieldInfoObj;
        String filedNameWithoutNull = filedName == null ? "" : filedName;
        String fieldAliasWithoutNull = fieldAlias == null ? "" : filedName;
        return filedNameWithoutNull.equals(fieldInfo.getFiledName()) && fieldAliasWithoutNull.equals(fieldInfo.getFieldAlias());
    }
}

