package org.linc.RPCServer.fieldsconverter.hqlparserresult;

import org.linc.RPCServer.fieldsconverter.info.DependentTable;
import org.linc.RPCServer.fieldsconverter.info.FieldInfo;

import java.util.ArrayList;

/**
 * 用于存储对 TOK_INSERT AST 节点分析的结果
 *
 * @author ihainan
 * @version 1.0
 */
public class InsertAnalyseResult {
    private ArrayList<DependentTable> dependentTables = new ArrayList<DependentTable>();
    private ArrayList<String> insertTables = null;
    private ArrayList<FieldInfo> allSelectedFields = new ArrayList<FieldInfo>();

    /**
     * 结果表各个字段所依赖的表，以及表所衍生的结果表中的字段
     *
     * @return 依赖表信息
     */
    public ArrayList<DependentTable> getDependentTables() {
        return dependentTables;
    }

    /**
     * 获取 INSERT INTO / INSERT OVERWRITE 从句需要插入表的真实表名
     *
     * @return 真实表名列表
     */
    public ArrayList<String> getInsertTables() {
        return insertTables;
    }

    /**
     * 获取结果表中的所有字段
     *
     * @return 结果表中的所有字段
     */
    public ArrayList<FieldInfo> getAllSelectedFields() {
        return allSelectedFields;
    }

    /**
     * 构造函数
     *
     * @param dependentTables   依赖表
     * @param allSelectedFields 结果表中的所有字段
     * @param insertTables      将被 INSERT INTO 或者 INSERT OVERWRITE 的表的表名的列表，可为空列表
     */
    public InsertAnalyseResult(ArrayList<DependentTable> dependentTables,
                               ArrayList<FieldInfo> allSelectedFields, ArrayList<String> insertTables) {
        this.dependentTables = dependentTables;
        this.insertTables = insertTables;
        this.allSelectedFields = allSelectedFields;
    }
}
