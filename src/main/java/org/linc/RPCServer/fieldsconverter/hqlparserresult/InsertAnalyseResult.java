package org.linc.RPCServer.fieldsconverter.hqlparserresult;

import org.linc.RPCServer.fieldsconverter.DependentTable;
import org.linc.RPCServer.fieldsconverter.FieldInfo;

import java.util.ArrayList;

/**
 * 用于存储对 INSERT AST 节点分析的结果
 * @author ihainan
 * @version 1.0
 */
public class InsertAnalyseResult {
    private ArrayList<DependentTable> dependentTables = new ArrayList<DependentTable>();
    private ArrayList<String> insertTables = null;
    private ArrayList<FieldInfo> allSelectedFields = new ArrayList<FieldInfo>();

    /**
     * FROM 从句中当前 QUERY 中所依赖的表
     * @return 依赖表
     */
    public ArrayList<DependentTable> getDependentTables() {
        return dependentTables;
    }

    /**
     * 获取 INSERT INTO / INSERT OVERWRITE 从句需要插入表的真实表名
     * @return 真实表名列表
     */
    public ArrayList<String> getInsertTables() {
        return insertTables;
    }

    /**
     * 获取 SELECT 从句中所有被选择的字段
     * @return
     */
    public ArrayList<FieldInfo> getAllSelectedFields() {
        return allSelectedFields;
    }

    /**
     * 构造函数
     * @param dependentTables
     * @param allSelectedFields
     * @param insertTables
     */
    public InsertAnalyseResult(ArrayList<DependentTable> dependentTables,
                               ArrayList<FieldInfo> allSelectedFields, ArrayList<String> insertTables){
        this.dependentTables = dependentTables;
        this.insertTables = insertTables;
        this.allSelectedFields = allSelectedFields;
    }
}
