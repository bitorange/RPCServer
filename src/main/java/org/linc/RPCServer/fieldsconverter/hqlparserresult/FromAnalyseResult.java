package org.linc.RPCServer.fieldsconverter.hqlparserresult;


import org.linc.RPCServer.fieldsconverter.TableInfo;

import java.util.ArrayList;

/**
 * 用于存储对 FROM AST 节点分析的结果
 * @author ihainan
 * @version 1.0
 */
public class FromAnalyseResult {
    /**
     * 获取 FROM 从句中，所有表的信息; <br/>
     * TableInfo 内部存储表名、别名和该表所有域。
     * @return FROM 从句中，所有表的信息
     */
    public ArrayList<TableInfo> getFromTablesInfo() {
        return fromTablesInfo;
    }

    private ArrayList<TableInfo> fromTablesInfo = new ArrayList<TableInfo>();

    /**
     * 构造函数
     * @param fromTablesInfo
     */
    public FromAnalyseResult(ArrayList<TableInfo> fromTablesInfo){
        this.fromTablesInfo = fromTablesInfo;
    }

}
