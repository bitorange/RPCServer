package linc.fieldsconverter;

/**
 * Created by ihainan on 3/30/15.
 */
public class QueueItem {
    /**
     * 需要继续搜索的表
     * @return 需要继续搜索的表
     */
    public TableInfo getTableToFind() {
        return tableToFind;
    }

    /**
     * 需要继续寻找的字段
     * @return 需要继续寻找的字段
     */
    public FieldInfo getFieldToFind() {
        return fieldToFind;
    }

    private TableInfo tableToFind;
    private FieldInfo fieldToFind;

    public QueueItem(TableInfo tableToFind, FieldInfo fieldToFind){
        this.tableToFind = tableToFind;
        this.fieldToFind = fieldToFind;
    }
}
