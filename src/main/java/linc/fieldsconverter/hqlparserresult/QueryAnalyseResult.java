package linc.fieldsconverter.hqlparserresult;

import linc.fieldsconverter.TableInfo;

/**
 * Created by ihainan on 3/30/15.
 */
public class QueryAnalyseResult {
    /**
     * 新生成表的信息
     * @return 新生成表的相关信息
     */
    public TableInfo getTableInfo() {
        return tableInfo;
    }

    /**
     * 获取 Query 内部 FROM 节点分析的结果
     * @return From 节点分析的结果
     */
    public FromAnalyseResult getFromAnalyseResult() {
        return fromAnalyseResult;
    }

    /**
     * 获取 Query 内部 INSERT 节点分析的结果
     * @return INSERT 节点分析的结果
     */
    public InsertAnalyseResult getInsertAnalyseResult() {
        return insertAnalyseResult;
    }

    public QueryAnalyseResult(TableInfo tableInfo, FromAnalyseResult fromAnalyseResult, InsertAnalyseResult insertAnalyseResult){
        this.tableInfo = tableInfo;
        this.fromAnalyseResult = fromAnalyseResult;
        this.insertAnalyseResult = insertAnalyseResult;
    }

    TableInfo tableInfo;
    FromAnalyseResult fromAnalyseResult;
    InsertAnalyseResult insertAnalyseResult;

}
