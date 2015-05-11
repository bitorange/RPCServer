package org.linc.RPCServer.fieldsconverter.info;

import java.util.ArrayList;

/**
 * Created by ihainan on 5/11/15.
 */
public class OriginalDependentTables {
    private ArrayList<OriginalDependentTable> originalDependentTableArrayList;
    private Boolean isIncludeFunctionOrExpression;

    /**
     * 获取某字段的原始依赖表列表
     * @return 原始依赖表列表
     */
    public ArrayList<OriginalDependentTable> getOriginalDependentTableArrayList() {
        return originalDependentTableArrayList;
    }

    /**
     * 该字段获得过程中是否经过了函数或者表达式
     * @return true 表示经过了函数或者表达式，否则为 false
     */
    public Boolean getIsIncludeFunctionOrExpression() {
        return isIncludeFunctionOrExpression;
    }

    /**
     * 构造函数
     * @param originalDependentTableArrayList 原始依赖表列表
     * @param isIncludeFunctionOrExpression 该字段获得过程中是否经过了函数或者表达式
     */
    public OriginalDependentTables(ArrayList<OriginalDependentTable> originalDependentTableArrayList, Boolean isIncludeFunctionOrExpression) {
        this.originalDependentTableArrayList = originalDependentTableArrayList;
        this.isIncludeFunctionOrExpression = isIncludeFunctionOrExpression;
    }
}
