package com.wacai.stanlee.omega.dataFormat;

import java.util.List;

/**
 * author guchi@wacai.com
 * create 2017-12-19 下午5:51
 **/
public class CategoricalSliceDOJava {
    private List<String> categoricalValues;

    private String fieldName;

    private String fieldType;

    public List<String> getCategoricalValues() {
        return categoricalValues;
    }

    public void setCategoricalValues(List<String> categoricalValues) {
        this.categoricalValues = categoricalValues;
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

    public CategoricalSliceDOJava(List<String> categoricalValues, String fieldName, String fieldType) {
        this.categoricalValues = categoricalValues;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
    }
}
