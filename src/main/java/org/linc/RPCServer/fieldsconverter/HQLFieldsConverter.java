package org.linc.RPCServer.fieldsconverter;

import javafx.scene.control.Tab;
import org.apache.commons.lang.StringUtils;
import org.linc.RPCServer.ConnectJDBC;
import org.linc.RPCServer.fieldsconverter.hqlparserresult.*;
import org.linc.RPCServer.fieldsconverter.info.*;
import org.linc.RPCServer.fieldsconverter.rule.*;
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
import java.sql.Types;
import java.util.*;

/**
 * 本类内部实现了字段规则转换算法，并对外提供字段转换接口
 *
 * @author ihainan
 * @version 1.1
 */
public class HQLFieldsConverter {
    /**
     * AST 节点值到 AST 节点名的映射
     * <p> HiveParser 中使用整型数值来表示 AST 节点类型，本 HashMap 存储节点数值到节点名的映射关系 </p>
     *
     * @see org.apache.hadoop.hive.ql.parse.HiveParser
     */
    private static HashMap<Integer, String> typeValueToNames;

    static {
        try {
            typeValueToNames = getTypeValueToNamesHashMap();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    /**
     * 到远程数据库的 JDBC 连接对象
     * <p>该对象被用于解析 AST 时，可获得某数据库表的字段信息 </p>
     */
    private java.sql.Connection con = null;

    /**
     * 类构造函数
     *
     * @param con 到远程数据库的 JDBC 连接对象
     */
    public HQLFieldsConverter(java.sql.Connection con) {
        this.con = con;
    }

    /**
     * 获取 typeValueToNames 哈希表
     *
     * @return typeValueToNames 哈希表
     * @throws ClassNotFoundException Classpath 中找不到 org.apache.hadoop.hive.ql.parse.HiveParser 类
     * @throws IllegalAccessException 不具备访问 org.apache.hadoop.hive.ql.parse.HiveParser 中某域的权限
     * @see org.apache.hadoop.hive.ql.parse.HiveParser
     */
    private static HashMap<Integer, String> getTypeValueToNamesHashMap() throws ClassNotFoundException, IllegalAccessException {
        HashMap<Integer, String> typeNames = new HashMap<Integer, String>();
        for (Field field : Class.forName("org.apache.hadoop.hive.ql.parse.HiveParser").getFields()) {
            if (Modifier.isFinal(field.getModifiers()) && field.getType() == int.class) {
                typeNames.put(field.getInt(null), field.getName());
            }
        }
        return typeNames;
    }

    /**
     * 根据 AST 节点类型值获取该节点的类型名
     *
     * @param typeInt AST 节点类型值
     * @return 节点的类型名，如果 typeValueToNames 中不存在该类型，则返回 “OTHERS”
     */
    private static String getASTNodeType(int typeInt) {
        if (typeValueToNames.containsKey(typeInt)) {
            return typeValueToNames.get(typeInt);
        } else {
            return "OTHER";
        }
    }

    /**
     * printASTree 函数中遍历 AST 的最大深度
     */
    private static final int MAX_DEPTH = 100;

    /**
     * 使用 DFS 深度遍历并打印一棵 AST
     *
     * @param tree  需要遍历的 AST
     * @param depth 打印的最大深度，大于该深度将不再往下
     */
    private static void printASTree(ASTNode tree, int depth) {
        if (depth > MAX_DEPTH)
            return;
        for (Node n : tree.getChildren()) {
            ASTNode asn = (ASTNode) n;
            for (int i = 0; i <= depth - 1; ++i)
                System.out.print("\t");
            System.out.print(getASTNodeType(asn.getToken().getType()));
            System.out.print("  " + asn.toString());
            System.out.println();
            if (asn.getChildCount() > 0) {
                printASTree(asn, depth + 1);
            }
        }
    }

    /**
     * 用于存储某张结果表所直接依赖的数据表（FROM 从句后接的数据库表或者中间表）的信息
     * <p>哈希表的 Key 由结果表调用 toString 方法得到 </p>
     * <p>哈希表的 Value 为所有依赖表构成的列表 </p>
     */
    private HashMap<String, ArrayList<DependentTable>> dependenceOfTables = new HashMap<String, ArrayList<DependentTable>>();

    /**
     * 用于存储一个规则文件内的所有规则
     */
    private Rules rules = new Rules();

    /**
     * 分析 TOK_QUERY 节点
     *
     * @param node   需要分析的 AST_QUERY 节点
     * @param isRoot 该 QUERY 是不是将生成最终的结果表，true 为是, false 为不是
     * @return TOK_QUERY 节点分析得到的结果
     * @see org.linc.RPCServer.fieldsconverter.hqlparserresult.QueryAnalyseResult
     */
    private QueryAnalyseResult queryAnalyse(ASTNode node, Boolean isRoot) throws Exception {
        /* 类型检查 */
        if (node.getToken().getType() != HiveParser.TOK_QUERY) {
            System.err.println("Err: 解析节点不是 TOK_QUERY 类型");
            return null;
        }

        /* 获取表名和别名 */
        String tableName, alias;
        if (isRoot) {
            tableName = TableInfo.FINAL_TABLE;
            alias = TableInfo.FINAL_TABLE;
        } else {
            // 子查询，只有别名
            tableName = null;
            ASTNode idNode = getChild((ASTNode) node.getParent(), "Identifier");
            alias = idNode.toString();
        }

        /* 分析 FROM 从句 */
        ASTNode fromNode = getChild(node, "TOK_FROM");
        FromAnalyseResult fromAnalyseResult = fromAnalyse(fromNode);
        if (fromAnalyseResult == null) {
            System.err.println("Err: 分析 FROM 从句失败");
            return null;
        }

        /* 分析 INSERT 从句 */
        ASTNode insertNode = getChild(node, "TOK_INSERT");
        InsertAnalyseResult insertAnalyseResult = insertAnalyse(insertNode, fromAnalyseResult.getFromTablesInfo());
        if (insertAnalyseResult == null) {
            System.err.println("Err: 分析 INSERT 从句失败");
            return null;
        }

        /* 根据 InsertAnalyseResult 和 FromAnalyseResult，将依赖信息登记到全局依赖表 */
        dependenceOfTables.put(tableName + ", " + alias, insertAnalyseResult.getDependentTables());  // 通过 tableName 和 alias 唯一标记一个 SQL 查询中的某张表

        /* 构建信息表 */
        TableInfo currentTableInfo = new TableInfo(tableName, alias,
                insertAnalyseResult.getAllSelectedFields());
        return new QueryAnalyseResult(currentTableInfo, fromAnalyseResult, insertAnalyseResult);
    }

    /**
     * 分析 TOK_INSERT 节点
     *
     * @param insertNode     需要分析的 TOK_INSERT 节点
     * @param fromTablesInfo 与 TOK_INSERT 同级 TOK_FROM 节点的分析结果
     * @return TOK_INSERT 分析得到的结果
     */
    private InsertAnalyseResult insertAnalyse(ASTNode insertNode, ArrayList<TableInfo> fromTablesInfo) {
        /* 类型检查 */
        if (insertNode.getToken().getType() != HiveParser.TOK_INSERT) {
            System.err.println("Err: 解析节点不是 TOK_INSERT 类型");
            return null;
        }

        ArrayList<FieldInfo> allSelectedFields = new ArrayList<FieldInfo>();    // 所有 SELECT 字段的汇总
        ArrayList<DependentTable> dependentTables = new ArrayList<DependentTable>();
        ArrayList<String> insertTables = new ArrayList<String>();

        // SELECT CLAUSE，需要获得得到所有的字段，以及字段所依赖的表
        ASTNode selectNode = getChild(insertNode, "TOK_SELECT");
        if (selectNode == null) {
            System.err.println("Err: TOK_INSERT 下不存在 SELECT 子节点");
            return null;
        }

        // 获取得到新表的所有字段
        int numOfColumns = -1;  // 用于获取无别名字段的隐藏别名（c_x）
        for (Node prNode : selectNode.getChildren()) {
            numOfColumns++;

            ASTNode prChild = (ASTNode) ((ASTNode) prNode).getChild(0);

            // * 形式或者 Table.*
            if (prChild.getToken().getType() == HiveParser.TOK_ALLCOLREF) {
                // *，不可能存在别名
                ASTNode tabNameNode = getChild(prChild, "TOK_TABNAME");
                if (tabNameNode == null) {
                    // 获取表名和别名以及所依赖的字段
                    if (fromTablesInfo.size() <= 0) {
                        System.err.println("Err: SELECT * 的情况下，fromTablesInfo 为空");
                        return null;
                    }
                    for (TableInfo tableInfo : fromTablesInfo) {
                        // 获取字段的直接来源表
                        ArrayList<FieldInfo> fields = tableInfo.getFields();
                        for (int i = 0; i < fields.size(); ++i) {
                            FieldInfo field = fields.get(i);
                            FieldInfo newField = new FieldInfo(field.getFiledName(), field.getFieldAlias(), tableInfo);
                            fields.set(i, newField);
                        }

                        DependentTable dependentTable = new DependentTable(tableInfo, fields);
                        dependentTables.add(dependentTable);

                        // 记录所有的字段
                        allSelectedFields.addAll(fields);
                    }

                    // * 说明新表只有一个字段，直接 break
                    break;
                }

                // Table.*，不可能存在别名
                else {
                    // 获取表名
                    String selectedTableNameOrAlias = getChild(tabNameNode, "Identifier").toString();

                    // 找到对应 TableInfo
                    TableInfo tableToFind = null;
                    for (TableInfo info : fromTablesInfo) {
                        if (selectedTableNameOrAlias.equals(info.getTableName())
                                || selectedTableNameOrAlias.equals(info.getAliasName())) {
                            tableToFind = info;
                            break;
                        }
                    }

                    // 更新 dependentTables
                    if (tableToFind != null) {
                        for (FieldInfo fieldInfo : tableToFind.getFields()) {
                            FieldInfo newFieldInfo = new FieldInfo(fieldInfo.getFiledName(), fieldInfo.getFieldAlias(), tableToFind);
                            dependentTables = this.createOrUpdateDependentTables(dependentTables, tableToFind, newFieldInfo);
                            allSelectedFields.add(newFieldInfo);
                        }
                    } else {
                        System.err.println("Err: 在 fromTablesInfo 中找不到名为 " + selectedTableNameOrAlias + " 的表");
                        return null;
                    }

                    // 可能还有其他字段，不 Break
                }
            } else {
                // NULL 形式，只能用在 INSERT INTO 或者 INSERT OVERWRITE 情况，在 dependentTables 某个特殊表中记录
                if (prChild.getToken().getType() == HiveParser.TOK_NULL) {
                    FieldInfo fieldInfo = new FieldInfo(null, null, null);
                    allSelectedFields.add(fieldInfo);
                    dependentTables = this.createOrUpdateDependentTables(dependentTables,
                            new TableInfo(TableInfo.NULL_TABLE, TableInfo.NULL_TABLE, null), fieldInfo);
                }

                // Table.Field 形式。Field 可能存在别名
                else if (prChild.getToken().getType() == HiveParser.DOT) {
                    // 获取表名和表别名
                    String selectedTableNameOrAlias = getChild(getChild(prChild, "TOK_TABLE_OR_COL"), "Identifier").toString();
                    TableInfo tableToFind = null;
                    for (TableInfo info : fromTablesInfo) {
                        if (selectedTableNameOrAlias.equals(info.getTableName())
                                || selectedTableNameOrAlias.equals(info.getAliasName())) {
                            tableToFind = info;
                            break;
                        }
                    }

                    if (tableToFind == null) {
                        System.err.println("Err: FROM 从句中找不到 Table.Field 想要寻找的 Table");
                        return null;
                    }

                    // 获取字段名
                    String fieldName = getChild(prChild, "Identifier").toString();
                    String fieldAlias = null;

                    // 获取字段别名
                    if (getChild((ASTNode) prChild.getParent(), "Identifier") != null) {
                        ASTNode identifierNode = getChild((ASTNode) prChild.getParent(), "Identifier");
                        fieldAlias = identifierNode.toString();
                    }
                    allSelectedFields.add(new FieldInfo(fieldName, fieldAlias, tableToFind));

                    // 更新
                    dependentTables = this.createOrUpdateDependentTables(dependentTables, tableToFind, new FieldInfo(fieldName, fieldAlias, tableToFind));
                }

                // 纯 Field 形式，Field 不可为 *，可能存在别名
                else if (prChild.getToken().getType() == HiveParser.TOK_TABLE_OR_COL) {
                    // 字段名和字段别名
                    String fieldName = getChild(prChild, "Identifier").toString();
                    String fieldAlias = null;
                    if (getChild((ASTNode) prChild.getParent(), "Identifier") != null) {
                        ASTNode identifierNode = getChild((ASTNode) prChild.getParent(), "Identifier");
                        fieldAlias = identifierNode.toString();
                    }

                    // 表名
                    TableInfo tableToFind = null;
                    for (TableInfo tableInfo : fromTablesInfo) {
                        if (tableInfo.findField(fieldName) != null
                                || tableInfo.findField(fieldAlias) != null) {
                            tableToFind = tableInfo;
                            break;
                        }
                    }

                    // 更新
                    if (tableToFind != null) {
                        allSelectedFields.add(new FieldInfo(fieldName, fieldAlias, tableToFind));
                        dependentTables = this.createOrUpdateDependentTables(dependentTables, tableToFind,
                                new FieldInfo(fieldName, fieldAlias, tableToFind));
                    } else {
                        System.err.println("Err: 无法在来源表信息中找到相应的字段");
                        return null;
                    }
                }

                // 函数或者表达式，可能存在别名
                else {
                    // 表名与表别名 - 未知
                    TableInfo tableInfo = new TableInfo(TableInfo.UNKNOWN_TABLE, TableInfo.UNKNOWN_TABLE_ALIAS, null);

                    // 获取字段名，并检测字段有无别名，如果没有，使用 c_x 表示
                    String fieldName = "c_" + numOfColumns;
                    String fieldAlias = "c_" + numOfColumns;
                    ASTNode prParent = (ASTNode) prChild.getParent();
                    if (getChild(prParent, "Identifier") != null) {
                        fieldAlias = getChild(prParent, "Identifier").toString();
                    }

                    // 解析，平摊，取出所有的TOK_TABLE_OR_COL，对应的表信息，以及 Filed（别名登记为 [字段别名_y]）
                    this.innerFieldNumber = 0;
                    dependentTables = getAllInnerFields(prChild, dependentTables, fromTablesInfo, fieldAlias);

                    // 更新 dependentTables，更新 UNKNOWN_TABLE 中存储的字段
                    dependentTables = this.createOrUpdateDependentTables(dependentTables, tableInfo, new FieldInfo(fieldName, fieldAlias, tableInfo));

                    // 更新 allSelectedFields，添加当前 FieldInfo
                    allSelectedFields.add(new FieldInfo(fieldName, fieldAlias, tableInfo));
                }
            }
        }

        // INSERT INTO CLAUSE
        ASTNode insertIntoNode = getChild(insertNode, "TOK_INSERT_INTO");
        if (insertIntoNode != null) {
            ASTNode tabNode = getChild(insertIntoNode, "TOK_TAB");
            ASTNode tabNameNode = getChild(tabNode, "TOK_TABNAME");
            ASTNode identifierNode = getChild(tabNameNode, "Identifier");
            insertTables.add(identifierNode.toString());
        }

        // INSERT OVERWRITE CLAUSE
        ASTNode destinationNode = getChild(insertNode, "TOK_DESTINATION");
        if (destinationNode != null) {
            ASTNode tabNode = getChild(destinationNode, "TOK_TAB");
            if (tabNode != null) {
                ASTNode tabNameNode = getChild(tabNode, "TOK_TABNAME");
                ASTNode identifierNode = getChild(tabNameNode, "Identifier");
                insertTables.add(identifierNode.toString());
            }
        }

        return new InsertAnalyseResult(dependentTables, allSelectedFields, insertTables);
    }

    /**
     * HIVE SQL 所支持操作符的列表
     */
    private static final String[] OPERATIONS_LIST = {"+", "-", "x", "/", "%", "&", "|", "^", "~"};

    /**
     * 计数器，用于在 getAllInnerFields 内统计已经遍历过的字段的数目
     */
    private int innerFieldNumber = 0;

    /**
     * 获取函数或者表达式内部所有出现的字段，以及字段直接来源的数据表
     *
     * @param node                   需要分析的 AST 节点（对应函数或者表达式）
     * @param oldDependentTables     所以依赖的数据表的信息，初始为空，不断更新
     * @param fromTablesInfo         AST_FROM 节点分析得到的结果
     * @param originalFieldAliasName 函数或者表达式的别名
     * @return 函数或者表达式内部所有出现的字段，以及字段直接来源的数据表
     */
    private ArrayList<DependentTable> getAllInnerFields(ASTNode node, ArrayList<DependentTable> oldDependentTables,
                                                        ArrayList<TableInfo> fromTablesInfo, String originalFieldAliasName) {
        for (Node child : node.getChildren()) {
            ASTNode childAST = (ASTNode) child;

            // 如果子节点是函数，或者表达式，递归
            if (childAST.toString().equals("TOK_FUNCTION")
                    || Arrays.asList(OPERATIONS_LIST).contains(childAST.toString())) {
                oldDependentTables = getAllInnerFields(childAST, oldDependentTables,
                        fromTablesInfo, originalFieldAliasName);
            }

            // 如果是 DOT
            else if (childAST.getToken().getType() == HiveParser.DOT) {
                // 获取表名和表别名
                String selectedTableNameOrAlias = getChild(getChild(childAST, "TOK_TABLE_OR_COL"), "Identifier").toString();
                TableInfo tableToFind = null;
                for (TableInfo info : fromTablesInfo) {
                    if (selectedTableNameOrAlias.equals(info.getTableName())
                            || selectedTableNameOrAlias.equals(info.getAliasName())) {
                        tableToFind = info;
                        break;
                    }
                }

                if (tableToFind == null) {
                    System.err.println("Err: FROM 从句中找不到 Table.Field 想要寻找的 Table");
                    return null;
                }

                // 获取字段名
                String fieldName = getChild(childAST, "Identifier").toString();

                // 更新 oldDependentTables
                oldDependentTables = this.createOrUpdateDependentTables(oldDependentTables, tableToFind,
                        new FieldInfo(fieldName, originalFieldAliasName + "_" + this.innerFieldNumber, tableToFind));
                this.innerFieldNumber++;
            }
            // TOK_TABLE_OR_COL
            else if (childAST.getToken().getType() == HiveParser.TOK_TABLE_OR_COL) {
                // 字段名和字段别名
                String fieldName = getChild(childAST, "Identifier").toString();

                // 表名
                TableInfo tableToFind = null;
                for (TableInfo tableInfo : fromTablesInfo) {
                    if (tableInfo.findField(fieldName) != null) {
                        tableToFind = tableInfo;
                        break;
                    }
                }

                // 更新
                if (tableToFind != null) {
                    oldDependentTables = this.createOrUpdateDependentTables(oldDependentTables, tableToFind,
                            new FieldInfo(fieldName, originalFieldAliasName + "_" + this.innerFieldNumber, tableToFind));
                    this.innerFieldNumber++;
                } else {
                    System.err.println("Err: 无法在来源表信息中找到相应的字段");
                    return null;
                }
            }
        }

        return oldDependentTables;
    }


    /**
     * 用于更新一个依赖表的列表，加入新字段
     *
     * @param dependentTables 需要更新依赖表列表
     * @param tableInfo       需要添加字段所依赖的表
     * @param newFieldInfo    需要添加的字段
     * @return 新的依赖表列表
     */
    public ArrayList<DependentTable> createOrUpdateDependentTables(ArrayList<DependentTable> dependentTables,
                                                                   TableInfo tableInfo, FieldInfo newFieldInfo) {
        /* 原列表已有该依赖表的信息，更新该表的字段列表 */
        int i;
        for (i = 0; i < dependentTables.size(); ++i) {
            DependentTable dependentTable = dependentTables.get(i);
            if (dependentTable.getTableInfo().toString().equals(tableInfo.toString())) {
                dependentTables.get(i).addNewField(newFieldInfo);
                break;
            }
        }

        /* 原列表没有该依赖表的信息，添加新的依赖表 */
        if (i == dependentTables.size() || dependentTables.size() == 0) {
            ArrayList<FieldInfo> fieldsInfo = new ArrayList<FieldInfo>();
            fieldsInfo.add(newFieldInfo);
            DependentTable dependentTable = new DependentTable(tableInfo, fieldsInfo);
            dependentTables.add(dependentTable);
        }
        return dependentTables;
    }

    /**
     * 分析 INSERT 从句
     *
     * @param fromNode 需要分析，类型为 TOK_FROM 的 AST Node
     * @return 分析结果，中途出现错误则将错误信息打印并返回 null
     */
    private FromAnalyseResult fromAnalyse(ASTNode fromNode) throws Exception {
        /* 类型检查 */
        if (fromNode.getToken().getType() != HiveParser.TOK_FROM) {
            System.err.println("Err: 解析节点不是 TOK_FROM 类型");
            return null;
        }

        ArrayList<TableInfo> fromTablesInfo = new ArrayList<TableInfo>();  // 存储 FROM 从句下所有表的信息
        /* 获取得到所有子表的信息表 */
        // 单表（单子查询）情况，TOK_TABREF / TOK_SUBQUERY
        if (getJoinNode(fromNode) == null) {
            if (getChild(fromNode, "TOK_TABREF") != null) {
                TableInfo tableInfo = getTableInfoOfNode(getChild(fromNode, "TOK_TABREF"));
                fromTablesInfo.add(tableInfo);  // 表
            } else if (getChild(fromNode, "TOK_SUBQUERY") != null) {
                ASTNode queryChildNode = getChild(getChild(fromNode, "TOK_SUBQUERY"), "TOK_QUERY");
                TableInfo tableInfo = queryAnalyse(queryChildNode, false).getTableInfo();
                fromTablesInfo.add(tableInfo);
            }
        } else {
            // 多表情况，TOK_JOIN -> TOK_TABREF / TOK_SUBQUERY / TOK_JOIN
            ASTNode joinNode = getJoinNode(fromNode);
            fromTablesInfo.addAll(joinAnalyse(joinNode));
        }
        return new FromAnalyseResult(fromTablesInfo);
    }

    /**
     * 分析 JOIN AST 列表，得到有序的来源表信息
     *
     * @param joinNode 需要分析的 JOIN AST 节点
     * @return JOIN 从句中涉及到的来源表
     * @throws Exception 从数据库中获取某数据表信息失败
     */
    private ArrayList<TableInfo> joinAnalyse(ASTNode joinNode) throws Exception {
        ArrayList<TableInfo> fromTablesInfo = new ArrayList<TableInfo>();

        // 类型检查
        if (!checkIsJOINTypeNode(joinNode)) {
            return null;
        }

        // 第一个子节点，可能是Join，子查询或者表
        ASTNode child0 = (ASTNode) joinNode.getChild(0);
        TableInfo tableInfo0;
        if (checkIsJOINTypeNode(child0)) {
            fromTablesInfo.addAll(joinAnalyse(child0));
        } else {
            if (child0.getToken().getType() == HiveParser.TOK_TABREF) {
                tableInfo0 = getTableInfoOfNode(child0);
                fromTablesInfo.add(tableInfo0);
            } else if (child0.getToken().getType() == HiveParser.TOK_SUBQUERY) {
                tableInfo0 = queryAnalyse(getChild(child0, "TOK_QUERY"), false).getTableInfo();
                fromTablesInfo.add(tableInfo0);
            } else {
                System.err.println("Error: Join 节点下第一个子节点不为 Join、子查询或者表");
                return null;
            }
        }


        // 第二个子节点，只可能是子查询或者表
        ASTNode child1 = (ASTNode) joinNode.getChild(1);
        TableInfo tableInfo1;

        // FROM 表
        if (child1.getToken().getType() == HiveParser.TOK_TABREF) {
            tableInfo1 = getTableInfoOfNode(child1);
            fromTablesInfo.add(tableInfo1);
        }
        // FROM 子查询
        else if (child1.getToken().getType() == HiveParser.TOK_SUBQUERY) {
            tableInfo1 = queryAnalyse(getChild(child1, "TOK_QUERY"), false).getTableInfo();
            fromTablesInfo.add(tableInfo1);
        } else {
            System.err.println("Error: Join 节点下第二个子节点不为子查询或者表");
            return null;
        }

        return fromTablesInfo;
    }


    /**
     * 所有类型的 JOIN 语句对应 AST 节点的类型名
     */
    private final static String[] JOIN_TYPE = {"TOK_JOIN", "TOK_LEFTOUTERJOIN", "TOK_RIGHTOUTERJOIN", "TOK_FULLOUTERJOIN"};

    /**
     * 获取 JOIN 类型的子节点
     *
     * @param parentNode 父亲节点
     * @return JOIN 类型的子节点
     */
    public static ASTNode getJoinNode(ASTNode parentNode) {
        for (String type : JOIN_TYPE) {
            if (getChild(parentNode, type) != null) {
                return getChild(parentNode, type);
            }
        }
        return null;
    }

    /**
     * 检查一个 AST 节点是否是 JOIN 类型节点
     *
     * @param node 需要检查的 AST 节点
     * @return 返回 true 说明是 JOIN 类型节点，false 不是
     */
    public static Boolean checkIsJOINTypeNode(ASTNode node) {
        if (node == null) {
            return false;
        }
        for (String type : JOIN_TYPE) {
            if (type.equals(node.getToken().getText())) {
                return true;
            }
        }
        return false;
    }

    /**
     * 获取某 AST Node 中首个类型为 fieldName 的子节点
     *
     * @param node      节点
     * @param fieldName 类型名
     * @return 类型为 fieldName 的子节点，若有多个则返回第一个，不存在返回 null
     */
    private static ASTNode getChild(ASTNode node, String fieldName) {
        if (node != null && node.getChildCount() > 0) {
            for (Node child : node.getChildren()) {
                ASTNode childNode = (ASTNode) child;
                if (getASTNodeType(childNode.getType()).equals(fieldName))
                    return childNode;
            }
        }
        return null;
    }

    /**
     * 根据 AST 节点信息，获取对应数据表的相关信息
     *
     * @param tableNode 需要查询的 AST 节点
     * @return AST 节点对应数据库表的相关信息
     * @throws Exception 从数据库中获取某数据表信息失败
     */
    private TableInfo getTableInfoOfNode(ASTNode tableNode) throws Exception {
        // 获取表名
        ASTNode tabNameNode = getChild(tableNode, "TOK_TABNAME");
        String tableName = getChild(tabNameNode, "Identifier").toString();

        // 获取别名
        String aliasName = null;
        if (getChild(tableNode, "Identifier") != null) {
            aliasName = getChild(tableNode, "Identifier").toString();
        }

        // 获取表中字段
        ArrayList<FieldInfo> fieldsInfo = getFieldsOfATable(tableName);

        return new TableInfo(tableName, aliasName, fieldsInfo);
    }

    /**
     * 获取数据库中某表所包含的所有字段
     *
     * @param tableName 需要查询的数据库表
     * @return 数据库表的所有字段的信息
     * @throws Exception 执行 SQL 语句失败
     */
    private ArrayList<FieldInfo> getFieldsOfATable(String tableName) throws Exception {
        ArrayList<FieldInfo> fields = new ArrayList<FieldInfo>();

        // Connect to Database
        ConnectJDBC connectJDBC = new ConnectJDBC();
        String command = "SELECT * FROM " + tableName;
        ResultSet resultSet = connectJDBC.getAndExecuteSQL(command, this.con);
        ResultSetMetaData metaData = resultSet.getMetaData();
        for (int i = 1; i <= metaData.getColumnCount(); ++i) {
            String columnName = metaData.getColumnName(i);
            fields.add(new FieldInfo(columnName, null, null));  // 对于数据库中的表，在 AST 中没有对应的节点
        }
        return fields;
    }


    /**
     * 添加一个字段到一张 OriginalDependentTable 列表中
     *
     * @param dependentOriginalTables 需要添加字段的 OriginalDependentTable 列表
     * @param tableInfo               该字段所归属的数据表的相关信息
     * @param fieldName               需要添加字段的字段名
     * @return 添加字段成功后的 OriginalDependentTable 列表
     */
    public ArrayList<OriginalDependentTable> addFieldIntoOriginalDependentTables(ArrayList<OriginalDependentTable> dependentOriginalTables,
                                                                                 TableInfo tableInfo, String fieldName) {
        /* 原列表已有该依赖表的信息，更新该表的字段列表 */
        int i;
        for (i = 0; i < dependentOriginalTables.size(); ++i) {
            OriginalDependentTable dependentTable = dependentOriginalTables.get(i);
            if (dependentTable.getTableInfo().toString().equals(tableInfo.toString())) {
                dependentOriginalTables.get(i).addNewField(fieldName);
                break;
            }
        }

        /* 原列表没有该依赖表的信息，添加新的依赖表 */
        if (i == dependentOriginalTables.size() || dependentOriginalTables.size() == 0) {
            ArrayList<String> fieldsInfo = new ArrayList<String>();
            fieldsInfo.add(fieldName);
            OriginalDependentTable dependentTable = new OriginalDependentTable(tableInfo, fieldsInfo);
            dependentOriginalTables.add(dependentTable);
        }
        return dependentOriginalTables;
    }

    /**
     * 使用 BFS 生成一张 OriginalDependentTable 列表，该列表存储结果表中字段所最终依赖的数据库表的信息
     *
     * @param item 封装最终结果表信息的队列单元
     * @return 存储结果表中字段所最终依赖的数据库表的信息的 OriginalDependentTable 列表
     */
    private OriginalDependentTables getOriginalDependentTables(QueueItem item) {
        ArrayList<OriginalDependentTable> originalDependentTableList = new ArrayList<OriginalDependentTable>();
        boolean isIncludeFunctionOrExpression = false;

        // 初始化队列
        Queue<QueueItem> queue = new LinkedList<QueueItem>();
        queue.add(item);

        // 开始寻找原始表
        DependentTable dependentTableToFind;
        while (queue.size() != 0) {
            // 从队列中取出需要寻找的字段和将要进行遍历的的依赖表 Key（表名 + "," + 别名）
            item = queue.remove();
            FieldInfo fieldToFind = item.getFieldToFind();
            TableInfo tableToFind = item.getTableToFind();

            // TOK_NULL
            if (fieldToFind.getFiledName() == null && fieldToFind.getFieldAlias() == null) {
                return null;
            }

            // 寻找包含该字段（名）的依赖表，需要保证字段来源于该表
            dependentTableToFind = findTable(fieldToFind, tableToFind.getTableName(), tableToFind.getAliasName());
            if (dependentTableToFind != null) {
                // 所依赖的表自身的信息
                TableInfo dependentTableInfo = dependentTableToFind.getTableInfo();

                // TABLE_NULL，说明为函数或者表达式，需要单独处理
                if (TableInfo.UNKNOWN_TABLE.equals(dependentTableInfo.getTableName())
                        && TableInfo.UNKNOWN_TABLE_ALIAS.equals(dependentTableInfo.getAliasName())) {

                    // 登记，经函数或者表达式得到
                    isIncludeFunctionOrExpression = true;

                    // UNKNOWN_TABLE 只是一个伪表，需要进一步得到真实依赖的表
                    int columnNum = 0;

                    // 找出依赖表
                    FieldInfo realFieldToFind = new FieldInfo(fieldToFind.getFiledName() + "_" + columnNum, fieldToFind.getFiledName() + "_" + columnNum, null);
                    DependentTable realDependentTableToFind = findTable(realFieldToFind,
                            tableToFind.getTableName(), tableToFind.getAliasName());
                    while (realDependentTableToFind != null) {
                        // 找出相应依赖字段
                        FieldInfo originalField = realDependentTableToFind.findField(realFieldToFind.getFiledName());
                        originalDependentTableList = this.addFieldIntoOriginalDependentTables(originalDependentTableList, realDependentTableToFind.getTableInfo(), originalField.getFiledName());

                        // 循环，直到找不到依赖表
                        columnNum++;
                        realFieldToFind = new FieldInfo(fieldToFind.getFiledName() + "_" + columnNum, fieldToFind.getFiledName() + "_" + columnNum, null);
                        realDependentTableToFind = findTable(realFieldToFind,
                                tableToFind.getTableName(), tableToFind.getAliasName());
                    }
                }
                // 一般情况，检查是否存在于 dependenceOfTables 中，不存在则说明该表只存在于数据库中，存储到 originalTables 里面
                else {
                    String tableName = dependentTableInfo.getTableName();
                    String aliasName = dependentTableInfo.getAliasName();
                    if (!dependenceOfTables.containsKey(tableName + ", " + aliasName)) {
                        // 获取原始字段名，保存
                        FieldInfo originalField = dependentTableToFind.findField(fieldToFind.getFiledName());
                        originalDependentTableList = this.addFieldIntoOriginalDependentTables(originalDependentTableList, dependentTableInfo, originalField.getFiledName());
                    } else {
                        // 否则，放入队列
                        FieldInfo originalField = dependentTableToFind.findField(fieldToFind.getFiledName());
                        item = new QueueItem(dependentTableToFind.getTableInfo(), originalField);
                        queue.add(item);
                    }
                }
            } else {
                System.err.println("Err: 找不到字段 (" + tableToFind.getAliasName() + ") 所对应的依赖表");
                return null;
            }
        }

        return new OriginalDependentTables(originalDependentTableList, isIncludeFunctionOrExpression);
    }

    /**
     * 在 dependenceOfTables ，某表的依赖列表中寻找包含某特定 field 的表
     *
     * @param field     需要查询的 field
     * @param tableName 需要查询表的表名
     * @param alias     需要查询表的别名
     * @return 依赖表，查询不到返回 null
     */
    private DependentTable findTable(FieldInfo field, String tableName, String alias) {
        if (field.getDirectFromTable() == null
                || tableName.equals(field.getDirectFromTable().getTableName()) && alias.equals(field.getDirectFromTable().getAliasName())) {
            for (DependentTable table : dependenceOfTables.get(tableName + ", " + alias)) {
                // 保证字段来源于该表
                // Direct From Table 如果为空，说明为包含函数或者表达式的字段，该字段名字唯一
                // if (field.getDirectFromTable() == null || field.getDirectFromTable().toString().equals(table.getTableInfo().toString())) {
                // 只用表名进行搜索，SELECT 中的字段别名是给上一层使用的
                if (table.findField(field.getFiledName()) != null) {
                    return table;
                }
                // }
            }
        }
        System.err.println("Err: 无法在依赖表中找到包含字段 " + field.toString() +
                " 的表");
        return null;
    }

    /**
     * 解析 SQL 命令，对 SQL 命令所得到的结果集应用规则
     *
     * @param command   需要解析的 SQL 命令
     * @param resultSet 执行 SQL 命令得到结果集
     * @return 应用规则后的结果
     * <p>每个 ArrayList<String> 表示结果集中的一条记录经过转换得到的结果 </p>
     * <p>每个 String 表示该记录中，某个字段的值，格式为： 字段名 + "\t" + 转换得到的新值 </p>
     */
    public ArrayList<ArrayList<String>> parseCommand(String command, ResultSet resultSet) {
        ArrayList<ArrayList<String>> finalResult = new ArrayList<ArrayList<String>>();
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
            if (getChild(tree, "TOK_QUERY") != null) {
                dependenceOfTables = new HashMap<String, ArrayList<DependentTable>>();

                // 分析 TOK_QUERY，得到结果
                QueryAnalyseResult queryAnalyseResult = queryAnalyse((ASTNode) tree.getChild(0), true);
                if (queryAnalyseResult == null) {
                    System.err.println("Err: 解析失败");
                    return null;
                }

                TableInfo finalTableInfo = queryAnalyseResult.getTableInfo();   // 最终结果表的相关信息
                InsertAnalyseResult insertAnalyseResult = queryAnalyseResult.getInsertAnalyseResult();  // INSERT TOK 节点的分析结果

                // 获取所有字段对应的原始表以及原始表中的字段
                ArrayList<OriginalDependentTables> allOriginalDependentTables = new ArrayList<OriginalDependentTables>();
                for (int i = 0; i < finalTableInfo.getFields().size(); ++i) {
                    // 获取原始表和字段
                    FieldInfo field = finalTableInfo.getFields().get(i);    // SELECT 第 i 个字段
                    OriginalDependentTables originalDependentTables = getOriginalDependentTables(new QueueItem(finalTableInfo, field));   // 该字段依赖的表(s)和表中的原始字段
                    allOriginalDependentTables.add(originalDependentTables);

                    // 存在 SELECT 字段和 INSERT INTO / OVERWRITE 字段，需要继承规则
                    if (originalDependentTables != null && insertAnalyseResult.getInsertTables().size() > 0
                            && insertAnalyseResult.getInsertTables().get(0) != null) {
                        ArrayList<FieldInfo> fieldsToInsert = getFieldsOfATable(insertAnalyseResult.getInsertTables().get(0));  // 只可能 Insert 一张表，所以取 0，获取该表的字段名

                        // 对于没一个依赖的(原始)表
                        for (OriginalDependentTable originalDependentTable : originalDependentTables.getOriginalDependentTableArrayList()) {
                            String selectedTable = originalDependentTable.getTableInfo().getTableName();
                            // 对于原始表中的每一个依赖的字段
                            for (String selectedField : originalDependentTable.getFields()) {
                                // 检测是否存在对应规则
                                Rule rule = rules.findRule(selectedTable, selectedField);
                                // 存在规则
                                if (rule != null) {
                                    // 添加规则
                                    Rule newRule = new Rule(insertAnalyseResult.getInsertTables().get(0),
                                            fieldsToInsert.get(i).getFiledName(), rule.getRex(), rule.getReplaceContent());
                                    rules.addNewRule(newRule);
                                }
                            }
                        }
                    }
                }

                // 对结果应用规则
                if (resultSet != null) {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    // 对于每一项结果
                    while (resultSet.next()) {
                        ArrayList<String> resultOfOneRow = new ArrayList<String>(); // 每一项行对应的结果

                        // 对于每一个字段
                        for (int i = 1; i <= metaData.getColumnCount(); ++i) {
                            String value = resultSet.getString(i), newContent;
                            // 二进制类型数据
                            if (metaData.getColumnType(i) == Types.BINARY) {
                                newContent = "* Binary Data *";
                            } else {
                                if (value == null) {
                                    newContent = "";
                                } else {
                                    OriginalDependentTables originalDependentTables = allOriginalDependentTables.get(i - 1);  // 当前字段所依赖的所有表、表字段
                                    newContent = value;

                                    // 每一张原始表
                                    boolean hasFoundRule = false;
                                    for (OriginalDependentTable originalDependentTable : originalDependentTables.getOriginalDependentTableArrayList()) {
                                        // 原始表的每一个依赖字段
                                        for (int j = 0; j < originalDependentTable.getFields().size(); ++j) {
                                            // 应用规则
                                            String filedName = originalDependentTable.getFields().get(j);
                                            // 函数或者表达式
                                            if (rules.findRule(originalDependentTable.getTableInfo().getTableName(), filedName) != null) {
                                                if (originalDependentTables.getIsIncludeFunctionOrExpression()) {
                                                    newContent = StringUtils.repeat("*", value.length());
                                                    hasFoundRule = true;
                                                    break;
                                                }
                                                newContent = rules.applyRules(originalDependentTable.getTableInfo().getTableName(), filedName, newContent);
                                            }
                                        }
                                        if (!hasFoundRule) {
                                            break;
                                        }
                                    }
                                }
                            }
                            TableInfo fromTable = queryAnalyseResult.getInsertAnalyseResult().getAllSelectedFields().get(i - 1).getDirectFromTable();
                            String tableName = fromTable.getTableName() == null ? (fromTable.getAliasName() == null ? "" : fromTable.getAliasName() + ".") : fromTable.getTableName() + ".";
                            resultOfOneRow.add(tableName
                                    + metaData.getColumnName(i) + "\t" + newContent);
                        }
                        finalResult.add(resultOfOneRow);
                    }
                }
            } else {
                // ONLY FOR TOK_CREATETABLE
                if (getChild(tree, "TOK_CREATETABLE") != null && getChild(getChild(tree, "TOK_CREATETABLE"), "TOK_QUERY") != null) {
                    // 获取新表的表名
                    ASTNode newTableNode = getChild(getChild(getChild(tree, "TOK_CREATETABLE"), "TOK_TABNAME"), "Identifier");
                    String newTableName = newTableNode.toString();

                    // 获取字段，与 TOK_QUERY 相同
                    ASTNode queryNode = getChild(getChild(tree, "TOK_CREATETABLE"), "TOK_QUERY");
                    dependenceOfTables = new HashMap<String, ArrayList<DependentTable>>();

                    // 分析 TOK_QUERY，得到结果
                    QueryAnalyseResult queryAnalyseResult = queryAnalyse(queryNode, true);
                    if (queryAnalyseResult == null) {
                        System.err.println("Err: 解析失败");
                        return null;
                    }

                    TableInfo finalTableInfo = queryAnalyseResult.getTableInfo();   // 最终结果表的相关信息

                    // 获取所有字段对应的原始表以及原始表中的字段
                    ArrayList<OriginalDependentTables> allOriginalDependentTables = new ArrayList<OriginalDependentTables>();
                    for (int i = 0; i < finalTableInfo.getFields().size(); ++i) {
                        // 获取原始表和字段
                        FieldInfo field = finalTableInfo.getFields().get(i);    // SELECT 第 i 个字段
                        OriginalDependentTables originalDependentTables = getOriginalDependentTables(new QueueItem(finalTableInfo, field));   // 该字段依赖的表(s)和表中的原始字段
                        allOriginalDependentTables.add(originalDependentTables);

                        for (OriginalDependentTable originalDependentTable : originalDependentTables.getOriginalDependentTableArrayList()) {
                            String selectedTable = originalDependentTable.getTableInfo().getTableName();
                            // 对于原始表中的每一个依赖的字段
                            for (String selectedField : originalDependentTable.getFields()) {
                                // 检测是否存在对应规则
                                Rule rule = rules.findRule(selectedTable, selectedField);
                                // 存在规则
                                if (rule != null) {
                                    // 添加规则
                                    Rule newRule = new Rule(newTableName,
                                            field.getFiledName(), rule.getRex(), rule.getReplaceContent());
                                    rules.addNewRule(newRule);
                                }
                            }
                        }
                    }
                }
                // 删除数据表
                else if (getChild(tree, "TOK_DROPTABLE") != null) {
                    ASTNode droppedTableNode = getChild(getChild(getChild(tree, "TOK_DROPTABLE"), "TOK_TABNAME"), "Identifier");
                    String droppedTableName = droppedTableNode.toString();

                    // 删除规则
                    rules.deleteRulesOfATable(droppedTableName);
                }

                // 修改数据表表名
                else if (getChild(tree, "TOK_ALTERTABLE_RENAME") != null) {
                    String oldTableName = getChild(tree, "TOK_ALTERTABLE_RENAME").getChild(0).toString();
                    String newTableName = getChild(tree, "TOK_ALTERTABLE_RENAME").getChild(1).toString();

                    // 修改表名
                    rules.renameTable(oldTableName, newTableName);
                }
                // 修改列名
                else if (getChild(tree, "TOK_ALTERTABLE_RENAMECOL") != null) {
                    String tableName = getChild(tree, "TOK_ALTERTABLE_RENAMECOL").getChild(0).toString();
                    String oldColumnName = getChild(tree, "TOK_ALTERTABLE_RENAMECOL").getChild(1).toString();
                    String newColumnName = getChild(tree, "TOK_ALTERTABLE_RENAMECOL").getChild(2).toString();

                    // 修改列名
                    rules.replaceColumnName(tableName, oldColumnName, newColumnName);
                }
                // TOD  O: 提前获取数据表内容
                else if (getChild(tree, "TOK_ALTERTABLE_REPLACECOLS") != null) {
                    // String tableName = getChild(tree, "TOK_ALTERTABLE_REPLACECOLS").getChild(0).toString();
                    // ArrayList<FieldInfo> originalFileds = getFieldsOfATable(tableName);
                    ASTNode colNodeLists = getChild(getChild(tree, "TOK_ALTERTABLE_REPLACECOLS"), "TOK_TABCOLLIST");
                    ArrayList<String> newFields = new ArrayList<String>();
                    for (Node child : colNodeLists.getChildren()) {
                        ASTNode node = (ASTNode) child;
                        if (node.toString().equals("TOK_TABCOL")) {
                            newFields.add(getChild(node, "Identifier").toString());
                        }
                    }
                } else if (resultSet != null) {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    while (resultSet.next()) {
                        ArrayList<String> resultOfOneRow = new ArrayList<String>();
                        for (int i = 1; i <= metaData.getColumnCount(); ++i) {
                            resultOfOneRow.add(metaData.getColumnName(i) + "\t" + resultSet.getString(i));
                        }
                        finalResult.add(resultOfOneRow);
                    }
                } else {
                    return finalResult;
                }
            }

        } catch (ParseException e) {
            System.err.println("Err: parse SQL command error " + e.getMessage());
            return null;
        } catch (SQLException e) {
            System.err.println("Err: Result Set get next result error " + e.getMessage());
            return null;
        } catch (Exception e) {
            System.err.println("Err: 未知错误 : " + e.getMessage());
            return null;
        }
        return finalResult;
    }
}
