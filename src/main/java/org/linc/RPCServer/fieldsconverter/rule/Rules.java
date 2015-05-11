package org.linc.RPCServer.fieldsconverter.rule;


import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;

import org.linc.RPCServer.*;

/**
 * 表示一个规则文件中的所有规则
 */
public class Rules {
    private ArrayList<Rule> allRules = new ArrayList<Rule>();
    private static String ruleFilePath = GlobalVar.configMap.get("rules.path");     // 规则文件存储路径


    /**
     * 构造函数，读取本地规则文件
     */
    public Rules() {
        this.readRules(ruleFilePath);
    }

    /**
     * 从本地文件中读取规则
     *
     * @return true 表示读取成功， false 表示读取失败
     */
    public Boolean readRules(String ruleFilePath) {
        allRules = new ArrayList<Rule>();

        // Open the file
        FileInputStream fstream;
        try {
            fstream = new FileInputStream(ruleFilePath);
        } catch (FileNotFoundException e) {
            System.err.println("Err: 规则文件 " + ruleFilePath + " 未找到");
            return false;
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

        String strLine;

        // Read file line by line
        try {
            while ((strLine = br.readLine()) != null) {
                String[] items = strLine.split("\t");
                if (items.length == 4) {
                    Rule rule = new Rule(items[0], items[1], items[2], items[3]);
                    allRules.add(rule);
                }
            }
        } catch (IOException e) {
            System.err.println("Err: 读取规则文件 " + ruleFilePath + " 失败");
            return false;
        }

        // Close the input stream
        try {
            br.close();
        } catch (IOException e) {
            System.err.println("Err: 关闭规则文件 " + ruleFilePath + " 失败");
            return false;
            // e.printStackTrace();
        }
        return true;
    }

    /**
     * 将规则列表中的应用到指定内容中
     *
     * @param tableName       内容来源表
     * @param columnName      内容来源列
     * @param originalContent 需要应用规则的内容
     * @return 应用规则后的新文本
     */
    public String applyRules(String tableName, String columnName, String originalContent) {
        String newContent = originalContent;
        for (Rule rule : allRules) {
            if (rule.checkAvailable(tableName, columnName)) {
                newContent = rule.applyRule(tableName, columnName, originalContent);
            }
        }
        return newContent;
    }

    /**
     * 添加新规则，并写入到文件中
     *
     * @param rule 需要添加的新规则
     * @return 是否添加成功，true 表示成功，false 表示失败
     * @throws IOException 写入规则文件失败
     */
    public void addNewRule(Rule rule) throws IOException {
        readRules(ruleFilePath);
        if (!isExisted(rule)) {
            allRules.add(rule);
            updateRulesFile(allRules);
        }
    }

    /**
     * 删除指定规则
     *
     * @param rule 需要删除的规则
     * @throws IOException 写入规则文件失败
     */
    public void deleteRule(Rule rule) throws IOException {
        int i = 0;
        for (; i < allRules.size(); ++i) {
            if (rule.toString().equals(allRules.get(i).toString())) {
                break;
            }
        }

        if (i != allRules.size()) {
            allRules.remove(i);
            updateRulesFile(allRules);
        }
    }


    /**
     * 删除指定表的所有规则
     * @param tableName 需要删除规则的表
     * @throws IOException 写入规则文件失败
     */
    public void deleteRulesOfATable(String tableName) throws IOException {
        Iterator<Rule> iterable = allRules.iterator();
        while (iterable.hasNext()) {
            Rule rule = iterable.next();
            if(rule.getTableName().equals(tableName)){
                iterable.remove();
            }
        }

        updateRulesFile(allRules);
    }

    /**
     * 更新规则文件
     *
     * @param newRules 需要写入的规则列表
     * @throws IOException 写入规则文件失败
     */
    public void updateRulesFile(ArrayList<Rule> newRules) throws IOException {
        Writer output = new BufferedWriter(new FileWriter(ruleFilePath, false));
        for (Rule rule : newRules) {
            output.append(rule.toString() + "\n");
        }
        output.close();
    }

    /**
     * 寻找特定表 - 列是否存在可使用的规则
     *
     * @param tableName  表名
     * @param columnName 列名
     * @return 返回找到的规则，否则返回 null
     */
    public Rule findRule(String tableName, String columnName) {
        for (Rule rule : allRules) {
            if (tableName.equals(rule.getTableName()) && columnName.equals(rule.getColumnName())) {
                return rule;
            }
        }
        return null;
    }

    /**
     * 查看规则是否已经存在
     *
     * @param rule 需要查找的规则
     * @return true 表示找到，false 表示找不到
     */
    private Boolean isExisted(Rule rule) {
        for (Rule r : allRules) {
            if (rule.toString().equals(r.toString())) {
                return true;
            }
        }
        return false;
    }
}
