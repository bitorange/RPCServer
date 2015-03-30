package linc.fieldsconverter;

import linc.ConnectJDBC;
import linc.fieldsconverter.hqlparserresult.*;
import linc.fieldsconverter.rule.*;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

/**
 * 用于解析 HQL 命令，以及进行字段转换
 * @author ihainan
 * @version 1.1
 */
public class HQLFieldsConverter {
    /** A hashmap used to store all the possible types of AST Nodes. **/
    private static HashMap<Integer, String> typeNames;
    static {
        try {
            typeNames = getAllASNodeType();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    private java.sql.Connection con = null;
    public HQLFieldsConverter(java.sql.Connection con) {
        this.con = con;
    }

    /** Get all the possible types of AST nodes **/
    private static
    HashMap<Integer, String> getAllASNodeType() throws ClassNotFoundException, IllegalAccessException {
        HashMap<Integer, String> typeNames = new HashMap<Integer, String>();
        for (Field field:Class.forName("org.apache.hadoop.hive.ql.parse.HiveParser").getFields()){
            if(Modifier.isFinal(field.getModifiers()) && field.getType() == int.class){
                typeNames.put(field.getInt(null), field.getName());
            }
        }
        return typeNames;
    }

    private static final int MAX_DEPTH = 100;
    /** Print an AST tree **/
    private static void printASTree(ASTNode tree, int depth){
        if(depth > MAX_DEPTH)
            return;
        for(Node n: tree.getChildren()){
            ASTNode asn = (ASTNode) n;
            for(int i = 0; i <= depth - 1; ++i)
                System.out.print("\t");
            System.out.print(getASTNodeType(asn.getToken().getType()));
            System.out.print("  " + asn.toString());
            System.out.println();
            if(asn.getChildCount() > 0) {
                printASTree(asn, depth + 1);
            }
        }
    }

    /** Get the type of an AST Node **/
    private static String getASTNodeType(int typeInt){
        if(typeNames.containsKey(typeInt)){
            return typeNames.get(typeInt);
        }
        else{
            return "OTHER";
        }
    }

    private HashMap<String, ArrayList<DependentTable>> dependenceOfTables = new HashMap<String, ArrayList<DependentTable>>();   // 用于存储某一张表所依赖的所有表的信息，信息包括依赖表表名、别名、字段列表，以及所依赖该依赖表的新字段列表
    private Rules rules = new Rules();  // 用于存储和应用规则

    /**
     * Analyse QUERY clause
     * Register the dependence info into DEPENDENCY table and return the info the new table
     **/
    private TableInfo queryAnalyse(ASTNode node, Boolean isRoot){
        /* 类型检查 */
        if(node.getToken().getType() != HiveParser.TOK_QUERY) {
            System.err.println("Err: 解析节点不是 TOK_QUERY 类型");
            return null;
        }

        /* 获取表名和别名 */
        String tableName, alias;
        if(isRoot){
            tableName = TableInfo.FINAL_TABLE;
            alias = TableInfo.FINAL_TABLE;
        }
        else{
            // 子查询，只有别名
            tableName = null;
            ASTNode idNode = getChild((ASTNode)node.getParent(), "Identifier");
            alias = idNode.toString();
        }

        /* 分析 FROM 从句 */
        ASTNode fromNode = getChild(node, "TOK_FROM");
        FromAnalyseResult fromAnalyseResult = fromAnalyse(fromNode);
        if(fromAnalyseResult == null){
            System.err.println("Err: 分析 FROM 从句失败");
            return null;
        }

        /* 分析 INSERT 从句 */
        ASTNode insertNode = getChild(node, "TOK_INSERT");
        InsertAnalyseResult insertAnalyseResult = insertAnalyse(insertNode, fromAnalyseResult.getFromTablesInfo());
        if(insertAnalyseResult == null){
            System.err.println("Err: 分析 INSERT 从句失败");
            return null;
        }

        /* 根据 InsertAnalyseResult 和 FromAnalyseResult，将依赖信息登记到全局依赖表 */
        dependenceOfTables.put(tableName + ", " + alias, insertAnalyseResult.getDependentTables());  // 通过 tableName 和 alias 唯一标记一个 SQL 查询中的某张表

        /* 构建信息表 */
        return new TableInfo(tableName, alias,
                insertAnalyseResult.getAllSelectedFields());
    }

    /**
     * 分析 INSERT 从句
     * @param insertNode 需要分析，类型为 TOK_INSERT 的 AST Node
     * @param fromTablesInfo fromAnalyse 分析得到的结果
     * @return 分析结果，中途出现错误则将错误信息打印并返回 null
     */
    private InsertAnalyseResult insertAnalyse(ASTNode insertNode, ArrayList<TableInfo> fromTablesInfo) {
        /* 类型检查 */
        if(insertNode.getToken().getType() != HiveParser.TOK_INSERT) {
            System.err.println("Err: 解析节点不是 TOK_INSERT 类型");
            return null;
        }

        ArrayList<FieldInfo> allSelectedFields = new ArrayList<FieldInfo>();  // 所有 SELECT 字段的汇总
        ArrayList<DependentTable> dependentTables = new ArrayList<DependentTable>();
        ArrayList<String> insertTables = new ArrayList<String>();

        // SELECT CLAUSE，需要获得得到所有的字段，以及字段所依赖的表
        ASTNode selectNode = getChild(insertNode, "TOK_SELECT");
        if (selectNode == null) {
            System.err.println("Err: TOK_INSERT 下不存在 SELECT 子节点");
            return null;
        }

        // TODO: 对函数、表达式的支持
        // 获取得到新表的所有字段
        int numOfColumns = -1;  // 用于获取无别名字段的隐藏别名（c_x）
        for (Node prNode : selectNode.getChildren()) {
            numOfColumns ++;

            // TODO: 错误检查
            ASTNode prChild = (ASTNode) ((ASTNode) prNode).getChild(0);

            // * 形式或者 Table.*
            if (prChild.getToken().getType() == HiveParser.TOK_ALLCOLREF) {
                // *
                ASTNode tabNameNode = getChild(prChild, "TOK_TABNAME");
                if(tabNameNode == null) {
                    // 获取表名和别名以及所依赖的字段
                    if (fromTablesInfo.size() <= 0) {
                        System.err.println("Err: SELECT * 的情况下，fromTablesInfo 为空");
                        return null;
                    }
                    TableInfo onlyTableInfo = fromTablesInfo.get(0);    // 只有一张表
                    DependentTable dependentTable = new DependentTable(onlyTableInfo, onlyTableInfo.getFields());
                    dependentTables.add(dependentTable);

                    // 记录所有的字段
                    allSelectedFields = dependentTable.getFields();

                    // 新表只有一个字段，直接 break
                    break;
                }
                // Table.*
                else{
                    // 获取表名
                    String selectedTableNameOrAlias = getChild(tabNameNode, "Identifier").toString();

                    // 找到对应 TableInfo
                    TableInfo tableToFind = null;
                    for(TableInfo info: fromTablesInfo) {
                        if (selectedTableNameOrAlias.equals(info.getTableName())
                                || selectedTableNameOrAlias.equals(info.getAliasName())) {
                            tableToFind = info;
                            break;
                        }
                    }

                    // 更新 dependentTables
                    if(tableToFind != null) {
                        for (FieldInfo fieldInfo : tableToFind.getFields()) {
                            dependentTables = this.addFieldIntoDependentTables(dependentTables, tableToFind, fieldInfo);
                            allSelectedFields.add(fieldInfo);
                        }
                    }
                    else{
                        System.err.println("Err: 在 fromTablesInfo 中找不到名为 " + selectedTableNameOrAlias + " 的表");
                        return null;
                    }

                    // 可能有多个字段，不 Break
                }
            }else {
                // NULL 形式，只能用在 INSERT INTO 或者 INSERT OVERWRITE 情况，在 dependentTables 某个特殊表中记录
                if(prChild.getToken().getType() == HiveParser.TOK_NULL){
                    FieldInfo fieldInfo = new FieldInfo(null, null, prChild);
                    allSelectedFields.add(fieldInfo);
                    dependentTables = this.addFieldIntoDependentTables(dependentTables,
                            new TableInfo(TableInfo.NULL_TABLE, TableInfo.NULL_TABLE, null), fieldInfo);
                }

                // Table.Field 形式。Field 可能存在别名
                else if (prChild.getToken().getType() == HiveParser.DOT) {
                    // 获取表名和表别名
                    String selectedTableNameOrAlias = getChild(getChild(prChild, "TOK_TABLE_OR_COL"), "Identifier").toString();
                    TableInfo tableToFind = null;
                    for(TableInfo info: fromTablesInfo){
                        if(selectedTableNameOrAlias.equals(info.getTableName())
                                || selectedTableNameOrAlias.equals(info.getAliasName())){
                            tableToFind = info;
                            break;
                        }
                    }

                    if(tableToFind == null){
                        System.err.println("Err: FROM 从句中找不到 Table.Field 想要寻找的 Table");
                        return null;
                    }

                    // 获取字段名
                    String fieldName = getChild(prChild, "Identifier").toString();
                    String fieldAlias = null;

                    // 获取字段别名
                    if(getChild((ASTNode)prChild.getParent(), "Identifier") != null) {
                        ASTNode identifierNode = getChild((ASTNode)prChild.getParent(), "Identifier");
                        if(identifierNode != null) {
                            fieldAlias = identifierNode.toString();
                        }
                        else {
                            System.err.println("Err: 别名节点不存在");
                            return null;
                        }
                    }
                    allSelectedFields.add(new FieldInfo(fieldName, fieldAlias, prChild));

                    // 更新
                    dependentTables = this.addFieldIntoDependentTables(dependentTables, tableToFind, new FieldInfo(fieldName, fieldAlias, prChild));
                }
                // 纯 Field 形式，Field 不可为 *
                else if (prChild.getToken().getType() == HiveParser.TOK_TABLE_OR_COL){
                    // 字段名和字段别名
                    String fieldName = getChild(prChild, "Identifier").toString();
                    String fieldAlias = null;
                    if(getChild((ASTNode)prChild.getParent(), "Identifier") != null) {
                        ASTNode identifierNode = getChild((ASTNode)prChild.getParent(), "Identifier");
                        if(identifierNode != null) {
                            fieldAlias = identifierNode.toString();
                        }
                        else {
                            System.err.println("Err: 别名节点不存在");
                            return null;
                        }
                    }
                    allSelectedFields.add(new FieldInfo(fieldName, fieldAlias, prChild));

                    // TODO: 不应该检索 fieldAlias
                    // 表名
                    TableInfo tableToFind = null;
                    for (TableInfo tableInfo: fromTablesInfo){
                        if(tableInfo.findField(fieldName) != null
                                || tableInfo.findField(fieldAlias) != null){
                            tableToFind = tableInfo;
                            break;
                        }
                    }

                    // 更新
                    if(tableToFind != null) {
                        dependentTables = this.addFieldIntoDependentTables(dependentTables, tableToFind,
                                new FieldInfo(fieldName, fieldAlias, prChild));
                    }
                    else{
                        System.err.println("Err: 无法在来源表信息中找到相应的字段");
                        return null;
                    }
                }
                // 函数或者表达式
                else{
                    // 表名与别名 - 未知
                    TableInfo tableInfo = new TableInfo(TableInfo.UNKNOWN_TABLE, TableInfo.UNKNOWN_TABLE_ALIAS, null);

                    // 检测字段有无别名，如果没有，使用 c_x 表示
                    String fieldName = "c_" + numOfColumns;
                    String fieldAlias = "c_" + numOfColumns;
                    ASTNode prParent = (ASTNode) prChild.getParent();
                    if(getChild(prParent, "Identifier") != null){
                        fieldAlias = getChild(prParent, "Identifier").toString();
                    }

                    // 解析，平摊，取出所有的TOK_TABLE_OR_COL，对应的表信息，以及 Filed（别名修改为 c_x_y）
                    this.innerFieldNumber = 0;
                    dependentTables = getAllInnerFields(prChild, dependentTables, fromTablesInfo, fieldAlias);

                    // 更新 dependentTables，添加 UNKNOWN_TABLE
                    dependentTables = this.addFieldIntoDependentTables(dependentTables, tableInfo, new FieldInfo(fieldName, fieldAlias, prChild));

                    // 更新 allSelectedFields，添加当前 FieldInfo
                    allSelectedFields.add(new FieldInfo(fieldName, fieldAlias, prChild));
                }
            }
        }

        // INSERT INTO CLAUSE
        ASTNode insertIntoNode = getChild(insertNode, "TOK_INSERT_INTO");
        if(insertIntoNode != null){
            ASTNode tabNode = getChild(insertIntoNode, "TOK_TAB");
            ASTNode tabNameNode = getChild(tabNode, "TOK_TABNAME");
            ASTNode identifierNode = getChild(tabNameNode, "Identifier");
            insertTables.add(identifierNode.toString());
        }

        // INSERT OVERWRITE CLAUSE
        ASTNode destinationNode = getChild(insertNode, "TOK_DESTINATION");
        if(destinationNode != null){
            ASTNode tabNode = getChild(destinationNode, "TOK_TAB");
            if(tabNode != null) {
                ASTNode tabNameNode = getChild(tabNode, "TOK_TABNAME");
                ASTNode identifierNode = getChild(tabNameNode, "Identifier");
                insertTables.add(identifierNode.toString());
            }
        }

        return new InsertAnalyseResult(dependentTables, allSelectedFields, insertTables);
    }

    // TODO: 扩展表达式列表
    // 表达式列表
    private static final String[] OPERATIONS_LIST = {"+", "-", "x", "/"};
    private int innerFieldNumber = 0;

    // 获取函数或者表达式内部的所有字段以及字段来源表信息
    private ArrayList<DependentTable> getAllInnerFields(ASTNode node, ArrayList<DependentTable> oldDependentTables,
                                                        ArrayList<TableInfo> fromTablesInfo, String originalFieldAliasName){
        // ArrayList<DependentTable> newDependentTables = new ArrayList<DependentTable>();
        for(Node child: node.getChildren()){
            ASTNode childAST = (ASTNode) child;

            // 如果子节点是函数，或者表达式，递归
            if(childAST.toString().equals("TOK_FUNCTION") || Arrays.asList(this.OPERATIONS_LIST).contains(childAST.toString())){
                oldDependentTables = getAllInnerFields(childAST, oldDependentTables,
                        fromTablesInfo, originalFieldAliasName);
            }

            // 如果是 DOT
            else if(childAST.getToken().getType() == HiveParser.DOT){
                // 获取表名和表别名
                String selectedTableNameOrAlias = getChild(getChild(childAST, "TOK_TABLE_OR_COL"), "Identifier").toString();
                TableInfo tableToFind = null;
                for(TableInfo info: fromTablesInfo){
                    if(selectedTableNameOrAlias.equals(info.getTableName())
                            || selectedTableNameOrAlias.equals(info.getAliasName())){
                        tableToFind = info;
                        break;
                    }
                }

                if(tableToFind == null){
                    System.err.println("Err: FROM 从句中找不到 Table.Field 想要寻找的 Table");
                    return null;
                }

                // 获取字段名
                String fieldName = getChild(childAST, "Identifier").toString();

                // 更新 oldDependentTables
                oldDependentTables = this.addFieldIntoDependentTables(oldDependentTables, tableToFind,
                        new FieldInfo(fieldName, originalFieldAliasName + "_" + this.innerFieldNumber, childAST));
                this.innerFieldNumber++;
            }
            // TOK_TABLE_OR_COL
            else if(childAST.getToken().getType() == HiveParser.TOK_TABLE_OR_COL){
                // 字段名和字段别名
                String fieldName = getChild(childAST, "Identifier").toString();

                // 表名
                TableInfo tableToFind = null;
                for (TableInfo tableInfo: fromTablesInfo){
                    if(tableInfo.findField(fieldName) != null){
                        tableToFind = tableInfo;
                        break;
                    }
                }

                // 更新
                if(tableToFind != null) {
                    oldDependentTables = this.addFieldIntoDependentTables(oldDependentTables, tableToFind,
                            new FieldInfo(fieldName, originalFieldAliasName + "_" + this.innerFieldNumber, childAST));
                    this.innerFieldNumber++;
                }
                else{
                    System.err.println("Err: 无法在来源表信息中找到相应的字段");
                    return null;
                }
            }
            else{
                // System.err.println("Err: getAllInnerFields 中出现特殊节点 " + childAST.toString());
                continue;
            }
        }

        return oldDependentTables;
    }


    /**
     * 用于更新一个依赖表的列表，加入新字段
     * @param dependentTables 依赖表列表
     * @param tableInfo 需要添加字段所依赖的表
     * @param fieldInfo 需要添加的字段
     * @return 新的依赖表列表
     */
    public ArrayList<DependentTable> addFieldIntoDependentTables(ArrayList<DependentTable> dependentTables,
                                                                 TableInfo tableInfo, FieldInfo fieldInfo){
        /* 原列表已有该依赖表的信息，更新该表的字段列表 */
        int i;
        for (i = 0; i < dependentTables.size(); ++i){
            DependentTable dependentTable = dependentTables.get(i);
            if(dependentTable.getTableInfo().toString().equals(tableInfo.toString())){
                dependentTables.get(i).addNewField(fieldInfo);
                break;
            }
        }

        /* 原列表没有该依赖表的信息，添加新的依赖表 */
        if(i == dependentTables.size() || dependentTables.size() == 0){
            ArrayList<FieldInfo> fieldsInfo = new ArrayList<FieldInfo>();
            fieldsInfo.add(fieldInfo);
            DependentTable dependentTable = new DependentTable(tableInfo, fieldsInfo);
            dependentTables.add(dependentTable);
        }
        return dependentTables;
    }

    /**
     * 分析 INSERT 从句
     * @param fromNode 需要分析，类型为 TOK_FROM 的 AST Node
     * @return 分析结果，中途出现错误则将错误信息打印并返回 null
     */
    private FromAnalyseResult fromAnalyse(ASTNode fromNode){
        /* 类型检查 */
        if(fromNode.getToken().getType() != HiveParser.TOK_FROM) {
            System.err.println("Err: 解析节点不是 TOK_FROM 类型");
            return null;
        }

        ArrayList<TableInfo> fromTablesInfo = new ArrayList<TableInfo>();  // 存储 FROM 从句下所有表的信息
        /* 获取得到所有子表的信息表 */
        // 单表（单子查询）情况，TOK_JOIN -> TOK_TABREF / TOK_SUBQUERY
        if(getJOINNode(fromNode) == null){
            if(getChild(fromNode, "TOK_TABREF") != null) {
                TableInfo tableInfo = getTableInfoOfNode(getChild(fromNode, "TOK_TABREF"));
                fromTablesInfo.add(tableInfo);  // 表
            }
            else if(getChild(fromNode, "TOK_SUBQUERY") != null){
                ASTNode queryChildNode = getChild(getChild(fromNode, "TOK_SUBQUERY"), "TOK_QUERY");
                TableInfo tableInfo = queryAnalyse(queryChildNode, false);
                fromTablesInfo.add(tableInfo);
            }
        }
        else {
            // 多表情况，TOK_JOIN -> TOK_TABREF / TOK_SUBQUERY / TOK_JOIN
            ASTNode joinNode = getJOINNode(fromNode);
            while(joinNode != null){
                // 第二个子节点，只可能是子查询或者表
                ASTNode child1 = (ASTNode)joinNode.getChild(1);

                // FROM 表
                if(child1.getToken().getType() == HiveParser.TOK_TABREF){
                    TableInfo tableInfo = getTableInfoOfNode(child1);
                    fromTablesInfo.add(tableInfo);
                }
                // FROM 子查询
                else if(child1.getToken().getType() == HiveParser.TOK_SUBQUERY){
                    TableInfo tableInfo = queryAnalyse(getChild(child1, "TOK_QUERY"), false);
                    fromTablesInfo.add(tableInfo);
                }
                else{
                    System.err.println("Error: Join 节点下第二个子节点不为子查询或者表");
                    return null;
                }

                // 第一个子节点，可能是Join，子查询或者表
                ASTNode child0 = (ASTNode)joinNode.getChild(0);
                if(checkIsJOINTypeNode(child0)){
                // if(child0.getToken().getType() == HiveParser.TOK_JOIN){
                    joinNode = child0;
                }
                else{
                    if(child0.getToken().getType() == HiveParser.TOK_TABREF){
                        TableInfo tableInfo = getTableInfoOfNode(child0);
                        fromTablesInfo.add(tableInfo);
                    }
                    else if(child0.getToken().getType() == HiveParser.TOK_SUBQUERY){
                        TableInfo tableInfo = queryAnalyse(getChild(child0, "TOK_QUERY"), false);
                        fromTablesInfo.add(tableInfo);
                    }
                    else{
                        System.err.println("Error: Join 节点下第一个子节点不为 Join、子查询或者表");
                        return null;
                    }
                    joinNode = null;
                }
            }
        }
        return new FromAnalyseResult(fromTablesInfo);
    }

    private final static String[] JOIN_TYPE = {"TOK_JOIN", "TOK_LEFTOUTERJOIN", "TOK_RIGHTOUTERJOIN", "TOK_FULLOUTERJOIN"};
    /**
     * 获取 JOIN 类型的子节点
     * @param parentNode 父亲节点
     * @return JOIN 类型的子节点
     */
    public static ASTNode getJOINNode(ASTNode parentNode){
        for(String type: JOIN_TYPE){
            if(getChild(parentNode, type) != null){
                return getChild(parentNode, type);
            }
        }
        return null;
    }

    /**
     * 检查一个节点是否是 JOIN 类型节点
     * @param node 需要检查的节点
     * @return 返回 true 说明是，否则不是
     */
    public static Boolean checkIsJOINTypeNode(ASTNode node){
        if(node == null){
            return false;
        }
        for(String type: JOIN_TYPE){
            if(type.equals(node.getToken().getText())){
                return true;
            }
        }
        return false;
    }

    /**
     * 获取某 AST Node 中类型为 fieldName 的子节点
     * @param node 节点
     * @param fieldName 类型名
     * @return 类型为 fieldName 的子节点，若有多个则返回第一个，不存在返回 null
     */
    private static ASTNode getChild(ASTNode node, String fieldName){
        if(node != null && node.getChildCount() > 0) {
            for (Node child : node.getChildren()) {
                ASTNode childNode = (ASTNode) child;
                if (getASTNodeType(childNode.getType()).equals(fieldName))
                    return childNode;
            }
        }
        return null;
    }

    /**
     * 根据节点信息，从数据库中获取一张表的相关信息
     * @param tableNode 需要获取信息的 TABLE 节点
     * @return 表信息，包括：<br/>
     * 1. 表名 <br/>
     * 2. 别名 <br/>
     * 3. 表中存在的字段 <br/>
     */
    private TableInfo getTableInfoOfNode(ASTNode tableNode){
        // TODO: 类型检查
        // 获取表名
        ASTNode tabNameNode = getChild(tableNode, "TOK_TABNAME");
        String tableName = getChild(tabNameNode, "Identifier").toString();

        // 获取别名
        String aliasName = null;
        if(getChild(tableNode, "Identifier") != null) {
            aliasName = getChild(tableNode, "Identifier").toString();
        }

        // 获取表中字段
        ArrayList<FieldInfo> fieldsInfo = getFieldsOfATable(tableName);

        return new TableInfo(tableName, aliasName, fieldsInfo);
    }

    /**
     * 获取数据中某表的所有字段名
     * @param tableName 表名
     * @return 字段名，中途出现错误则将错误信息打印并返回 null
     */
    private ArrayList<FieldInfo> getFieldsOfATable(String tableName){
        // TODO: 考虑表不存在的情况
        ArrayList<FieldInfo> fields = new ArrayList<FieldInfo>();

        // TODO: 性能优化
        // Connect to Database
        ConnectJDBC connectJDBC = new ConnectJDBC();
        String command = "SELECT * FROM " + tableName;
        ResultSet resultSet = connectJDBC.getAndExucuteSQL(command, this.con);
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            for(int i = 1; i <= metaData.getColumnCount(); ++i){
                String columnName = metaData.getColumnName(i);
                fields.add(new FieldInfo(columnName, null, null));  // 对于数据库中的表，在 AST 中没有对应的节点
            }
        } catch (SQLException e) {
            System.err.println("Err: 获取 metaData 失败");
            return null;
        }

        return fields;
    }


    /**
     *
     * @param dependentOriginalTables
     * @param tableInfo
     * @param fieldName
     * @return
     */
    public ArrayList<OriginalDependentTable> addFieldIntoOriginalDependentTables(ArrayList<OriginalDependentTable> dependentOriginalTables,
                                                                                 TableInfo tableInfo, String fieldName){
        /* 原列表已有该依赖表的信息，更新该表的字段列表 */
        int i;
        for (i = 0; i < dependentOriginalTables.size(); ++i){
            OriginalDependentTable dependentTable = dependentOriginalTables.get(i);
            if(dependentTable.getTableInfo().toString().equals(tableInfo.toString())){
                dependentOriginalTables.get(i).addNewField(fieldName);
                break;
            }
        }

        /* 原列表没有该依赖表的信息，添加新的依赖表 */
        if(i == dependentOriginalTables.size() || dependentOriginalTables.size() == 0){
            ArrayList<String> fieldsInfo = new ArrayList<String>();
            fieldsInfo.add(fieldName);
            OriginalDependentTable dependentTable = new OriginalDependentTable(tableInfo, fieldsInfo);
            dependentOriginalTables.add(dependentTable);
        }
        return dependentOriginalTables;
    }

    private ArrayList<OriginalDependentTable> getOriginalDependentTables(QueueItem item){
        ArrayList<OriginalDependentTable> originalDependentTables = new ArrayList<OriginalDependentTable>();

        // 初始化队列
        Queue queue = new LinkedList();
        queue.add(item);

        // 开始寻找原始表
        DependentTable dependentTableToFind;
        while(queue.size() != 0){
            // 从队列中取出需要寻找的字段和将要进行遍历的的依赖表 Key（表名 + "," + 别名）
            item = (QueueItem)queue.remove();
            FieldInfo fieldToFind = item.getFieldToFind();
            TableInfo tableToFind = item.getTableToFind();

            // 寻找包含该字段（名）的依赖表
            dependentTableToFind = findTable(fieldToFind, tableToFind.getTableName(), tableToFind.getAliasName());
            if(dependentTableToFind != null){
                // 所依赖的表自身的信息
                TableInfo dependentTableInfo = dependentTableToFind.getTableInfo();

                // TABLE_NULL，说明为函数或者表达式，需要单独处理
                if(TableInfo.UNKNOWN_TABLE.equals(dependentTableInfo.getTableName())
                        && TableInfo.UNKNOWN_TABLE_ALIAS.equals(dependentTableInfo.getAliasName())){
                    // UNKNOWN_TABLE 只是一个伪表，需要进一步得到真实依赖的表
                    int columnNum = 0;

                    // TODO: 复杂化 Field_Alias，避免冲突
                    // 找出依赖表
                    FieldInfo realFieldToFind = new FieldInfo(fieldToFind.getFiledName() + "_" + columnNum, fieldToFind.getFiledName() + "_" + columnNum, null);
                    DependentTable realDependentTableToFind = findTable(realFieldToFind,
                            tableToFind.getTableName(), tableToFind.getAliasName());
                    while(realDependentTableToFind != null){
                        // 找出相应依赖字段
                        FieldInfo originalField = realDependentTableToFind.findField(realFieldToFind.getFiledName());
                        originalDependentTables = this.addFieldIntoOriginalDependentTables(originalDependentTables, realDependentTableToFind.getTableInfo(), originalField.getFiledName());

                        // 循环，直到找不到依赖表
                        columnNum++;
                        realFieldToFind = new FieldInfo(fieldToFind.getFiledName() + "_" + columnNum, fieldToFind.getFiledName() + "_" + columnNum, null);
                        realDependentTableToFind = findTable(realFieldToFind,
                                tableToFind.getTableName(), tableToFind.getAliasName());
                    }
                }
                // 一般情况，检查是否存在于 dependenceOfTables 中，不存在则说明该表只存在于数据库中，存储到 originalTables 里面
                else{
                    String tableName = dependentTableInfo.getTableName();
                    String aliasName = dependentTableInfo.getAliasName();
                    if(!dependenceOfTables.containsKey(tableName + ", " + aliasName)){
                        // 获取原始字段名，保存
                        FieldInfo originalField = dependentTableToFind.findField(fieldToFind.getFiledName());
                        originalDependentTables = this.addFieldIntoOriginalDependentTables(originalDependentTables, dependentTableInfo, originalField.getFiledName());
                    }
                    else{
                        // 否则，放入队列
                        FieldInfo originalField = dependentTableToFind.findField(fieldToFind.getFiledName());
                        item = new QueueItem(dependentTableToFind.getTableInfo(), originalField);
                        queue.add(item);
                    }
                }
            }
            else{
                System.err.println("Err: 找不到字段 (" + tableToFind.getAliasName().toString() + ") 所对应的依赖表");
                return null;
            }
        }

        return originalDependentTables;
    }

    /**
     * 找出一条 SQL 查询语句中，某个 SELECT 字段依赖的表，该表必须存在于数据库中，而不是是中间生成的表
     * @param field 需要查询的字段
     * @return 最终所有依赖的表的列表
     */
    private ArrayList<DependentTable> getOriginalTables(FieldInfo field){
        // TODO: 对 Field 进行解析，可能是函数或者表达式，得到 Fields
        ArrayList<DependentTable> originalTables = new ArrayList<DependentTable>();

        // 目前仅考虑 Filed 不包含表达式、函数的情况，但留扩展接口
        String tableName = TableInfo.FINAL_TABLE;
        String alias = TableInfo.FINAL_TABLE;
        DependentTable dependentTableToFind = null;

        // TODO: 考虑字段为表达式（或函数）情况，可能会返回多张表
        while(dependenceOfTables.containsKey(tableName + ", " + alias)){
            // 如果 Field 为表达式，此处可以得到多张 table
            dependentTableToFind = findTable(field, tableName, alias);
            if(dependentTableToFind == null){
                break;
            }

            // 如果是 TABLE_UNKNOWN，获取所有 c_x_y 的表与字段
                // 判断 (dependentTableToFind.tableName + ", " + dependentTableToFind.alias) 是否存在于 dependenceOfTables，不存在，将 dependentTableToFind 放入 originalTables 中
                // 否则，将 Field 和 dependentTableToFind 放入队列中
            // 如果不是，将 Field 和 dependentTableToFind 放入队列中

            field = dependentTableToFind.findField(field.getFiledName());
            tableName = dependentTableToFind.getTableInfo().getTableName();
            alias = dependentTableToFind.getTableInfo().getAliasName();
        }

        if(dependentTableToFind == null){
            System.err.println("Err: 找不到字段 (" + field.toString() + ") 所对应的依赖表");
            return null;
        }
        else{
            originalTables.add(dependentTableToFind);
        }

        return originalTables;
    }

    /**
     * 在 dependenceOfTables ，某表的依赖列表中寻找包含某特定 field 的表
     * @param field 需要查询的 field
     * @param tableName 需要查询表的表名
     * @param alias 需要查询表的别名
     * @return 依赖表，查询不到返回 null
     */
    private DependentTable findTable(FieldInfo field, String tableName, String alias){
        for(DependentTable table: dependenceOfTables.get(tableName + ", " + alias)){
            // 只用表名进行搜索，SELECT 中的字段别名是给上一层使用的
            if(table.findField(field.getFiledName()) != null){
                return table;
            }
        }
        System.err.println("Err: 无法在依赖表中找到包含该 field 的表");
        return null;
    }

    /**
     * 将 SQL 命令执行的结果进行字段转换
     * @param command 需要执行的命令
     * @param resultSet 执行 SQL 语句得到的结果
     * @return 转换后的结果，中途出现错误输出错误并返回 null:<br/>
     * 1. Key: 字段名 <br/>
     * 2. Value: 转换后的值
     */
    public ArrayList<HashMap<String, String>> parseCommand(String command, ResultSet resultSet){
        ArrayList<HashMap<String, String>> finalResult = new ArrayList<HashMap<String, String>>();
        ParseDriver pd = new ParseDriver();
        try {
            /** Start analysing AS tree **/
            System.out.println("\n------------------ Parsing SQL Command ------------------");
            ASTNode tree = pd.parse(command);

            /** Print the AS tree **/
            System.out.println("\n------------------ AS Tree ------------------");
            printASTree(tree, 0);

            /** Start converting fields */
            // ONLY FOR TOK_QUERY!
            if(getChild(tree, "TOK_QUERY") != null) {
                dependenceOfTables = new HashMap<String, ArrayList<DependentTable>>();

                /** Analyse Query ASTNode **/
                TableInfo finalTableInfo = queryAnalyse((ASTNode) tree.getChild(0), true);
                if(finalTableInfo == null){
                    System.err.println("Err: 解析失败");
                    return null;
                }

                /** Get final result after being converted **/
                if (resultSet != null) {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    while (resultSet.next()) {
                        // Convert all the columns of one row
                        HashMap<String, String> resultOfOneRow = new HashMap<String, String>();
                        for (int i = 1; i <= finalTableInfo.getFields().size(); ++i) {
                            String value = resultSet.getString(i);
                            FieldInfo field = finalTableInfo.getFields().get(i - 1);
                            if (value == null) {
                                value = "";
                            }

                            // TODO: 多张表的情况的处理 [TEST]
                            ArrayList<OriginalDependentTable> originalDependentTables = getOriginalDependentTables(new QueueItem(finalTableInfo, field));
                            String newContent = value;
                            for(OriginalDependentTable originalDependentTable: originalDependentTables){
                                // 应用规则
                                for(int j = 0; j < originalDependentTable.getFields().size(); ++j) {
                                    String fileName = originalDependentTable.getFields().get(j);
                                    newContent = rules.applyRules(originalDependentTable.getTableInfo().getTableName(), fileName, newContent);
                                }
                            }

                            resultOfOneRow.put(metaData.getColumnName(i), newContent);

                            /*
                            ArrayList<DependentTable> tables = getOriginalTables(field);   // 搜索时候，不用考虑别名
                            DependentTable t = tables.get(0);

                            // 应用规则
                            String newContent = rules.applyRules(t.getTableInfo().getTableName(), field.getFiledName(), value);
                            resultOfOneRow.put(metaData.getColumnName(i), newContent);
                            */
                        }
                        finalResult.add(resultOfOneRow);
                    }
                } else {
                    return finalResult;
                }
            }
            else{
                if (resultSet != null) {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    while (resultSet.next()) {
                        HashMap<String, String> resultOfOneRow = new HashMap<String, String>();
                        for(int i = 1; i <= metaData.getColumnCount(); ++i){
                            resultOfOneRow.put(metaData.getColumnName(i), resultSet.getString(i));
                        }
                        finalResult.add(resultOfOneRow);
                    }
                }
                else{
                    return finalResult;
                }
            }
        } catch (ParseException e) {
            System.err.println("Err: parse SQL command error");
            return null;
        } catch (SQLException e) {
            System.err.println("Err: Result Set get next result error");
            return null;
        }
        return finalResult;
    }
}