package org.linc.RPCServer.fieldsconverter.info;

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
    private FieldInfo dependentField;

    /**
     * 当前字段所依赖的字段
     *
     * @return 字段所依赖的字段信息
     */
    public FieldInfo getDependentField() {
        return dependentField;
    }


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
     * @param fieldName       字段名
     * @param fieldAlias      字段别名
     * @param directFromTable 直接来源表
     * @param dependentField  字段所依赖的字段信息
     */
    public FieldInfo(String fieldName, String fieldAlias, TableInfo directFromTable, FieldInfo dependentField) {
        this.fieldAlias = fieldAlias;
        this.filedName = fieldName;
        this.directFromTable = directFromTable;
        this.dependentField = dependentField;
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

