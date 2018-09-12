package com.wacai.stanlee.omega.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wacai.stanlee.omega.dataFormat.*;
import scala.io.BufferedSource;
import scala.io.Source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * author guchi@wacai.com
 * create 2017-12-19 下午4:57
 **/
public class BangJsonUtils {
    private String tableName;

    private String tableDesc;
    private String labelName;
    private int partitionNum = 1;

//    private List<FieldInfoJava> fieldInfos;
    private List<String> primaryKeys;
    private List<NumericSliceDOJava> numericSliceDOs;

    private List<CategoricalSliceDOJava> categoricalSliceDOs;

    private List<DateSliceDOJava> dateSliceDOs;
    private List<UserDefinedSliceDOJava> userDefinedSliceDOs;

    private Map<String,List<String>> fieldAggMethods;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableDesc() {
        return tableDesc;
    }

    public void setTableDesc(String tableDesc) {
        this.tableDesc = tableDesc;
    }

//    public List<FieldInfoJava> getFieldInfos() {
//        return fieldInfos;
//    }
//
//    public void setFieldInfos(List<FieldInfoJava> fieldInfos) {
//        this.fieldInfos = fieldInfos;
//    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    public List<NumericSliceDOJava> getNumericSliceDOs() {
        return numericSliceDOs;
    }

    public void setNumericSliceDOs(List<NumericSliceDOJava> numericSliceDOs) {
        this.numericSliceDOs = numericSliceDOs;
    }

    public List<CategoricalSliceDOJava> getCategoricalSliceDOs() {
        return categoricalSliceDOs;
    }

    public void setCategoricalSliceDOs(List<CategoricalSliceDOJava> categoricalSliceDOs) {
        this.categoricalSliceDOs = categoricalSliceDOs;
    }

    public List<DateSliceDOJava> getDateSliceDOs() {
        return dateSliceDOs;
    }

    public void setDateSliceDOs(List<DateSliceDOJava> dateSliceDOs) {
        this.dateSliceDOs = dateSliceDOs;
    }

    public List<UserDefinedSliceDOJava> getUserDefinedSliceDOs() {
        return userDefinedSliceDOs;
    }

    public void setUserDefinedSliceDOs(List<UserDefinedSliceDOJava> userDefinedSliceDOs) {
        this.userDefinedSliceDOs = userDefinedSliceDOs;
    }

    public Map<String, List<String>> getFieldAggMethods() {
        return fieldAggMethods;
    }

    public void setFieldAggMethods(Map<String, List<String>> fieldAggMethods) {
        this.fieldAggMethods = fieldAggMethods;
    }

    public String getLabelName() {
        return labelName;
    }

    public void setLabelName(String labelName) {
        this.labelName = labelName;
    }

    public int getPartitionNum() {
        return partitionNum;
    }

    public void setPartitionNum(int partitionNum) {
        this.partitionNum = partitionNum;
    }

    public static BangJsonUtils fromFile(String fileName) {
        BufferedSource jsonStr =
                Source.fromInputStream(BangJsonUtils.class.getClassLoader().getResourceAsStream(fileName), "utf-8");
        return BangJsonUtils.fromJson(jsonStr.mkString());
    }

    public static BangJsonUtils fromJson(String json) {
        return JSONObject.parseObject(json, BangJsonUtils.class);
    }

    public static void main(String[] args) {
        BangJsonUtils bangJsonUtils = BangJsonUtils.fromFile("tableDescription.json");
        System.out.println(bangJsonUtils.getTableName());
//        List<>
//        Map<String,List<String>> map=new HashMap<>();
//        map.put("a",new ArrayList<String>(){{add("a");}});
//        map.put("b",new ArrayList<String>(){{add("b");}});
//        System.out.println(JSON.toJSONString(map));

    }
}
