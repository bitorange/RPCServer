package linc.fieldsconverter;

import org.apache.hadoop.hive.ql.parse.ASTNode;

/**
 * Created by ihainan on 3/27/15.
 */
public class FieldInfo {
    /**
     * 获取字段名
     * @return 字段名
     */
    public String getFiledName() {
        return filedName;
    }

    /**
     * 获取字段别名
     * @return 字段别名
     */
    public String getFieldAlias() {
        return fieldAlias;
    }

    private String filedName;
    private String fieldAlias;
    private ASTNode fieldNode;

    /**
     * 构造函数
     * @param fieldName  字段名
     * @param fieldAlias 字段别名
     */
    public FieldInfo(String fieldName, String fieldAlias, ASTNode fieldNode) {
        this.fieldAlias = fieldAlias;
        this.filedName = fieldName;
        this.fieldNode = fieldNode;
    }

    /**
     * 获取 Field 对应在 AST 中的节点
     * @return 节点
     */
    public ASTNode getFieldNode() {
        return fieldNode;
    }

    /**
     * 输出字段
     * @return 输出的字符串
     */
    @Override
    public String toString() {
        return this.filedName + ", " + this.fieldAlias;
    }

    /**
     * 比较两个字段是否相同
     * @param fieldInfoObj 需要比较的对象
     * @return true 表示相等，否则不等
     */
    @Override
    public boolean equals(Object fieldInfoObj) {
        FieldInfo fieldInfo = (FieldInfo) fieldInfoObj;
        String filedNameWithoutNull = filedName == null ? "" : filedName;
        String fieldAliasWithoutNull = fieldAlias == null ? "" : filedName;
        return filedNameWithoutNull.equals(fieldInfo.getFiledName()) && fieldAliasWithoutNull.equals(fieldInfo.getFieldAlias());
    }
}

