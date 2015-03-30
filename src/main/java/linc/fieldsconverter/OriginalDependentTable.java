package linc.fieldsconverter;

import java.util.ArrayList;

/**
 * Created by ihainan on 3/30/15.
 */
public class OriginalDependentTable {
    /**
     * 所依赖的原始表的信息
     * @return 原始表的信息
     */
    public TableInfo getTableInfo() {
        return tableInfo;
    }

    /**
     * 所依赖的原始表的字段
     * @return 原始表的字段
     */
    public ArrayList<String> getFields() {
        return fields;
    }

    /**
     * 构造函数
     * @param tableInfo 依赖表信息
     * @param fields 依赖表字段
     */
    public OriginalDependentTable(TableInfo tableInfo, ArrayList<String> fields){
        this.tableInfo = tableInfo;
        this.fields = fields;
    }

    /**
     * 添加新字段到字段列表中
     * @param field 需要添加的新字段
     */
    public void addNewField(String field){
        if(!fields.contains(field)){
            fields.add(field);
        }
    }

    private TableInfo tableInfo;
    private ArrayList<String> fields;
}
