package linc.fieldsconverter.rule;


import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by ihainan on 3/20/15.
 */
public class Rule {
    /**
     * 获取本规则适用表的表名
     * @return 表名
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * 获取本规则适用字段的字段名
     * @return 字段名
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * 获取本规则替换内容，使用正则表达式表示
     * @return 正则表达式
     */
    public String getRex() {
        return rex;
    }

    /**
     * 获取本规则替换新内容
     * @return 替换内容
     */
    public String getReplaceContent() {
        return replaceContent;
    }

    private String tableName;
    private String columnName;
    private String rex;
    private String replaceContent;

    /**
     * 构造函数
     * @param tableName 表名
     * @param columnName 列名
     * @param rex 正则表达式
     * @param replaceContent 替换内容
     */
    public Rule(String tableName, String columnName, String rex, String replaceContent){
        this.tableName = tableName;
        this.columnName = columnName;
        this.rex = rex;
        this.replaceContent = replaceContent;
    }


    /**
     * 输出规则内容
     * @return
     */
    @Override
    public String toString() {
        return this.tableName + "\t" + this.columnName + "\t" + this.rex + "\t" + this.replaceContent;
    }

    /**
     * 将规则应用到指定内容中
     * @param tableName 内容来源表
     * @param columnName 内容来源列
     * @param originalContent 需要应用规则的内容
     * @return 应用规则后的新文本
     */
    public String applyRule(String tableName, String columnName, String originalContent){
        String newContent;
        if(checkAvaliable(tableName, columnName)) {
            Pattern pattern = Pattern.compile(this.rex);
            Matcher matcher = pattern.matcher(originalContent);
            newContent = matcher.replaceAll(this.replaceContent);
        }
        else {
            System.err.println("Err: 表或列名不符合");
            return null;
        }
        return newContent;
    }

    /**
     * 检查规则是否适用于某表某列
     * @param tableName 查询表的表名
     * @param columnName 查询列的列名
     * @return true 表示适用，否则不适用
     */
    public Boolean checkAvaliable(String tableName, String columnName){
        return tableName.equals(this.tableName) && columnName.equals(this.columnName);
    }
}
