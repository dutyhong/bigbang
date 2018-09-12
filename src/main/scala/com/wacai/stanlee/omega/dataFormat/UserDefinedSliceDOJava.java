package com.wacai.stanlee.omega.dataFormat;

/**
 * @author manshahua@wacai.com
 * @date 2017/12/20 上午9:58
 */
public class UserDefinedSliceDOJava {
    private double min = 0;
    private double max = 1;
    private int sliceNum = 5;
    private String fieldName = null;
    private String fieldType = null;
    private String expression = null;

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

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public UserDefinedSliceDOJava(double min, double max, int sliceNum, String fieldName, String fieldType, String expression) {
        this.min = min;
        this.max = max;
        this.sliceNum = sliceNum;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.expression = expression;
    }
}
