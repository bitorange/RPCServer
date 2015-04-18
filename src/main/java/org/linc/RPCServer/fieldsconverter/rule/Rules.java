package org.linc.RPCServer.fieldsconverter.rule;

import java.io.*;
import java.util.ArrayList;
import java.util.ResourceBundle;

/**
 * Created by ihainan on 3/20/15.
 */
public class Rules {
    private ArrayList<Rule> allRules = new ArrayList<Rule>();
    private static String ruleFilePath;     // 规则文件存储路径，在 conf.properties 中指定

    static{
        ruleFilePath = ResourceBundle.getBundle("conf").getString("rulesFile");
    }

    /**
     * 构造函数，读取本地规则文件
     */
    public Rules(){
        this.readRules();
    }

    /**
     * 从本地文件中读取规则
     * @return true 表示读取成功，否则表示失败
     */
    public Boolean readRules(){
        allRules = new ArrayList<Rule>();

        // Open the file
        FileInputStream fstream = null;
        try {
            fstream = new FileInputStream(ruleFilePath);
        } catch (FileNotFoundException e) {
            System.err.println("Err: 规则文件 " + ruleFilePath + " 未找到");
            return false;
            // e.printStackTrace();
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

        String strLine;

        // Read file line by line
        try {
            while ((strLine = br.readLine()) != null)   {
                String[] items = strLine.split("\t");
                if(items.length == 4) {
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
     * @param tableName 内容来源表
     * @param columnName 内容来源列
     * @param originalContent 需要应用规则的内容
     * @return 应用规则后的新文本
     */
    public String applyRules(String tableName, String columnName, String originalContent){
        String newContent = originalContent;
        for(Rule rule: allRules){
            if(rule.checkAvaliable(tableName, columnName)){
                newContent = rule.applyRule(tableName, columnName, originalContent);
            }
        }
        return newContent;
    }

    /**
     * 添加新规则，并写入到文件中
     * @param rule
     * @return
     */
    public Boolean addNewRule(Rule rule){
        readRules();
        if(!isExisted(rule)){
            allRules.add(rule);

            // 写入文件
            try {
                Writer output = new BufferedWriter(new FileWriter(this.ruleFilePath, true));
                output.append(rule.toString());
                output.close();
            } catch (IOException e) {
                System.err.println("Err: 打开文件失败");
                return false;
            }

        }

        return true;
    }

    /**
     * 寻找特定表 - 列是否存在可使用的规则
     * @param tableName 表名
     * @param columnName 列名
     * @return 返回找到的规则，否则返回 null
     */
    public Rule findRule(String tableName, String columnName){
        for(Rule rule: allRules){
            if(tableName.equals(rule.getTableName()) && columnName.equals(rule.getColumnName())){
                return rule;
            }
        }
        return null;
    }

    /**
     * 查看规则是否已经存在
     * @param rule
     * @return
     */
    private Boolean isExisted(Rule rule){
        for(Rule r:allRules){
            if(rule.toString().equals(r.toString())){
                return true;
            }
        }
        return false;
    }
}
