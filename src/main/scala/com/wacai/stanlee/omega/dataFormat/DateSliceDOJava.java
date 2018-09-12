package com.wacai.stanlee.omega.dataFormat;

/**
 * author guchi@wacai.com
 * create 2017-12-19 下午5:55
 **/
public class DateSliceDOJava {
    private String sliceType;

    //    时间切分间隔，n月n天n年
    private int sliceTypeInterval;
    private int  sliceNum;
    //每次计算时间切片时的结束时间
    private String endTime;
    private String fieldName;

    private String fieldType;

    public String getSliceType() {
        return sliceType;
    }

    public void setSliceType(String sliceType) {
        this.sliceType = sliceType;
    }

    public int getSliceTypeInterval() {
        return sliceTypeInterval;
    }

    public void setSliceTypeInterval(int sliceTypeInterval) {
        this.sliceTypeInterval = sliceTypeInterval;
    }

    public int getSliceNum() {
        return sliceNum;
    }

    public void setSliceNum(int sliceNum) {
        this.sliceNum = sliceNum;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
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

    public DateSliceDOJava(String sliceType, int sliceTypeInterval, int sliceNum, String endTime, String fieldName, String fieldType) {
        this.sliceType = sliceType;
        this.sliceTypeInterval = sliceTypeInterval;
        this.sliceNum = sliceNum;
        this.endTime = endTime;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
    }
}
