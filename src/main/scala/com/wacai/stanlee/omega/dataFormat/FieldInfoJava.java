package com.wacai.stanlee.omega.dataFormat;

/**
 * author guchi@wacai.com
 * create 2017-12-19 下午5:54
 **/
public class FieldInfoJava {
    private String fieldName;

    private String fieldType;

    private Boolean isPrimaryKey;

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

    public Boolean getPrimaryKey() {
        return isPrimaryKey;
    }

    public void setPrimaryKey(Boolean primaryKey) {
        isPrimaryKey = primaryKey;
    }

    public FieldInfoJava(String fieldName, String fieldType, Boolean isPrimaryKey) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.isPrimaryKey = isPrimaryKey;
    }
}
