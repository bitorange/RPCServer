package org.linc.RPCServer.fieldsconverter.hqlparserresult;

import org.linc.RPCServer.fieldsconverter.info.TableInfo;

/**
 * 用于存储对 TOK_QUERY AST 节点分析的结果
 *
 * @author ihainan
 * @version 1.0
 */
public class QueryAnalyseResult {
    /**
     * 结果表的信息
     *
     * @return 新生成表的相关信息
     */
    public TableInfo getTableInfo() {
        return tableInfo;
    }

    /**
     * 获取 TOK_FROM 子节点分析的结果
     *
     * @return TOK_FROM 子节点分析的结果
     */
    public FromAnalyseResult getFromAnalyseResult() {
        return fromAnalyseResult;
    }

    /**
     * 获取TOK_INSERT 子节点分析的结果
     *
     * @return TOK_INSERT 子节点分析的结果
     */
    public InsertAnalyseResult getInsertAnalyseResult() {
        return insertAnalyseResult;
    }

    /**
     * 构造函数
     *
     * @param tableInfo           结果表的信息
     * @param fromAnalyseResult   TOK_FROM 子节点分析的结果
     * @param insertAnalyseResult TOK_INSERT 子节点分析的结果
     */
    public QueryAnalyseResult(TableInfo tableInfo, FromAnalyseResult fromAnalyseResult, InsertAnalyseResult insertAnalyseResult) {
        this.tableInfo = tableInfo;
        this.fromAnalyseResult = fromAnalyseResult;
        this.insertAnalyseResult = insertAnalyseResult;
    }

    private TableInfo tableInfo;
    private FromAnalyseResult fromAnalyseResult;
    private InsertAnalyseResult insertAnalyseResult;

}
