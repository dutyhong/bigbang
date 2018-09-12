package com.wacai.stanlee.omega.dataFormat;

/**
 * author guchi@wacai.com
 * create 2017-12-19 下午5:57
 **/
public class NumericSliceDOJava {
    //    最小值
    private double min;

    //    最大值
    private double max;

    //    切分成几段
    private int  sliceNum;

    //    切分的字段名
    private String fieldName;

    //    切分的字段类型
    private String fieldType;

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    public int getSliceNum() {
        return sliceNum;
    }

    public void setSliceNum(int sliceNum) {
        this.sliceNum = sliceNum;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldType() {
        return fieldType;
    }

    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }

    public NumericSliceDOJava(double min, double max, int sliceNum, String fieldName, String fieldType) {
        this.min = min;
        this.max = max;
        this.sliceNum = sliceNum;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
    }
}
