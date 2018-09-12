package com.wacai.stanlee.omega.featureselection;

import org.apache.spark.sql.*;

import java.util.List;

/**
 * @author manshahua@wacai.com
 * @date 2017/12/25 下午2:33
 */
public class FeatureSelectorParam {
    private String tableName = null;
    private String labelName = null;
    private List<String> otherColumnNames = null;
    private String[] featureColumnNames = null;
    private Dataset<Row> samples = null;
    private int originalFeatureNum = 0;
    private int selectedFeatureNum = 0;
    private double selectedFeatureThreshold = 0.0;
    private String selectMethod = null;

    public void init(FeatureTable featureTable, SparkSession sparkSession)
    {
        tableName = featureTable.getTableName();
        labelName = featureTable.getLabelName();
        otherColumnNames = featureTable.getOtherColumnNames();
        selectedFeatureNum = featureTable.getSelectedFeatureNum();
        selectedFeatureThreshold = featureTable.getSelectedFeatureThreshold();
        DataFrameReader dataFrameReader = new DataFrameReader(sparkSession);
        //samples = sparkSession.read().load(BigBangMain.filePath()+featureTable.getTableName());//
        samples =  dataFrameReader.table(featureTable.getTableName());
        String[] allColumns = samples.columns();
        int allFeatureNum = allColumns.length - (null==otherColumnNames?0:otherColumnNames.size());
        featureColumnNames = new String[allFeatureNum];
        int j = 0;
        for(int i=0; i<allColumns.length; i++)
        {
            if(!otherColumnNames.contains(allColumns[i]))
            {
                featureColumnNames[j] = allColumns[i];
                j++;
            }
        }
        selectMethod = featureTable.getSelectMethod();
    }

    public String[] getFeatureColumnNames() {
        return featureColumnNames;
    }

    public void setFeatureColumnNames(String[] featureColumnNames) {
        this.featureColumnNames = featureColumnNames;
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

    public Dataset<Row> getSamples() {
        return samples;
    }

    public void setSamples(Dataset<Row> samples) {
        this.samples = samples;
    }

    public int getOriginalFeatureNum() {
        return originalFeatureNum;
    }

    public void setOriginalFeatureNum(int originalFeatureNum) {
        this.originalFeatureNum = originalFeatureNum;
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

    public String getSelectMethod() {
        return selectMethod;
    }

    public void setSelectMethod(String selectMethod) {
        this.selectMethod = selectMethod;
    }
}
