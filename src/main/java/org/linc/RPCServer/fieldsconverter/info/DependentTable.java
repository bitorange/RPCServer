package org.linc.RPCServer.fieldsconverter.info;

import java.util.ArrayList;


/**
 * 用于表示结果表中的字段，对于 FROM 从句后的某张表的直接依赖情况
 */
public class DependentTable {
    /**
     * 获取其他表依赖于依赖表的所有字段
     *
     * @return 其他表依赖于依赖表的所有字段
     */
    public ArrayList<FieldInfo> getFields() {
        return fields;
    }

    /**
     * 获取所依赖的表的信息
     *
     * @return 依赖表的表信息
     */
    public TableInfo getTableInfo() {
        return tableInfo;
    }

    private TableInfo tableInfo;    // 所依赖表的表信息
    private ArrayList<FieldInfo> fields;   // 该表依赖的其他表，以及依赖表中对应的字段

    /**
     * 构造函数
     *
     * @param tableInfo 所依赖表的信息
     * @param fields    结果表中，依赖于依赖表的相应字段
     */
    public DependentTable(TableInfo tableInfo, ArrayList<FieldInfo> fields) {
        this.tableInfo = tableInfo;
        this.fields = fields;
    }

    /**
     * 添加新字段
     *
     * @param field 需要添加的字段
     */
    public void addNewField(FieldInfo field) {
        if (!fields.contains(field)) {
            fields.add(field);
        }
    }

    /**
     * 返回表名 + ", " + 别名
     *
     * @return 表名 + ", " + 别名
     */
    @Override
    public String toString() {
        return tableInfo.getTableName() + ", " + tableInfo.getAliasName();
    }


    /**
     * 寻找特定名称的字段
     *
     * @param fieldNameOrAlias 需要查找的字段名或者别名
     * @return 找到的字段，找不到返回 null
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
