package org.linc.RPCServer.fieldsconverter.info;

/**
 * BFS 搜索时候，所维护队列中的成员
 */
public class QueueItem {
    /**
     * 需要继续搜索的表
     *
     * @return 需要继续搜索的表
     */
    public TableInfo getTableToFind() {
        return tableToFind;
    }

    /**
     * 需要继续寻找的字段
     *
     * @return 需要继续寻找的字段
     */
    public FieldInfo getFieldToFind() {
        return fieldToFind;
    }

    private TableInfo tableToFind;
    private FieldInfo fieldToFind;

    public QueueItem(TableInfo tableToFind, FieldInfo fieldToFind) {
        this.tableToFind = tableToFind;
        this.fieldToFind = fieldToFind;
    }
}
