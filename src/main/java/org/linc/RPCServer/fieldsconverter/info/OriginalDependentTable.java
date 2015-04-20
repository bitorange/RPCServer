package org.linc.RPCServer.fieldsconverter.info;

import java.util.ArrayList;

/**
 * 用于表示结果表中的字段，对于数据库中直接存在的表的直接或者间接依赖情况
 */
public class OriginalDependentTable {
    /**
     * 获取所依赖的数据库中的表的信息
     *
     * @return 所依赖的数据库中的表的信息
     */
    public TableInfo getTableInfo() {
        return tableInfo;
    }

    /**
     * 获取结果表中，依赖于该数据库中表的所有的字段
     *
     * @return 结果表中，依赖于该数据库中表的所有的字段
     */
    public ArrayList<String> getFields() {
        return fields;
    }

    /**
     * 构造函数
     *
     * @param tableInfo 依赖表信息
     * @param fields    依赖表字段
     */
    public OriginalDependentTable(TableInfo tableInfo, ArrayList<String> fields) {
        this.tableInfo = tableInfo;
        this.fields = fields;
    }

    /**
     * 添加新字段到字段列表中
     *
     * @param field 需要添加的新字段
     */
    public void addNewField(String field) {
        if (!fields.contains(field)) {
            fields.add(field);
        }
    }

    private TableInfo tableInfo;
    private ArrayList<String> fields;
}
