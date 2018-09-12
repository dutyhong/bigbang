package com.wacai.stanlee.omega.featureselection;

import com.alibaba.fastjson.JSONObject;
import scala.io.BufferedSource;
import scala.io.Source;

import java.util.List;

/**
 * @author manshahua@wacai.com
 * @date 2017/12/25 下午2:21
 */
public class FeatureTable {
    private String tableName = null;
    private String labelName = null;
    private List<String> otherColumnNames = null;
    private int selectedFeatureNum = 0;
    private double selectedFeatureThreshold = 0.0;
    private String selectMethod = null;
//    public FeatureTable(){}
//    public FeatureTable(String tableName, String labelName, List<String> otherColumnNames, selec)
//    {
//        this.tableName = tableName;
//        this.labelName = labelName;
//        this.otherColumnNames = otherColumnNames;
//    }

    public String getSelectMethod() {
        return selectMethod;
    }

    public void setSelectMethod(String selectMethod) {
        this.selectMethod = selectMethod;
    }

    public int getSelectedFeatureNum() {
        return selectedFeatureNum;
    }

    public void setSelectedFeatureNum(int selectedFeatureNum) {
        this.selectedFeatureNum = selectedFeatureNum;
    }

    public double getSelectedFeatureThreshold() {
        return selectedFeatureThreshold;
    }

    public void setSelectedFeatureThreshold(double selectedFeatureThreshold) {
        this.selectedFeatureThreshold = selectedFeatureThreshold;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getLabelName() {
        return labelName;
    }

    public void setLabelName(String labelName) {
        this.labelName = labelName;
    }

    public List<String> getOtherColumnNames() {
        return otherColumnNames;
    }

    public void setOtherColumnNames(List<String> otherColumnNames) {
        this.otherColumnNames = otherColumnNames;
    }
    public static FeatureTable fromFile(String fileName) {
        BufferedSource jsonStr =
                Source.fromInputStream(FeatureTable.class.getClassLoader().getResourceAsStream(fileName), "utf-8");
        return FeatureTable.fromJson(jsonStr.mkString());
    }

    public static FeatureTable fromJson(String json) {
        return JSONObject.parseObject(json, FeatureTable.class);
    }

}
