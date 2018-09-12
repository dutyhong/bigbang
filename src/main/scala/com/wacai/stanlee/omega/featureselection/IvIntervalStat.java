package com.wacai.stanlee.omega.featureselection;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author manshahua@wacai.com
 * @date 2018/1/26 下午6:52
 */
public class IvIntervalStat {
    private String fieldName = null;
    private Map<String, String> intervalInfos = new TreeMap<>();
    private boolean isMonotone = true;
    public IvIntervalStat(String fieldName, boolean isMonotone, Map<String, String> intervalInfos) {
        this.fieldName = fieldName;
        this.isMonotone = isMonotone;
        this.intervalInfos = intervalInfos;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public Map<String, String> getIntervalInfos() {
        return intervalInfos;
    }

    public void setIntervalInfos(Map<String, String> intervalInfos) {
        this.intervalInfos = intervalInfos;
    }

    public boolean isMonotone() {
        return isMonotone;
    }

    public void setMonotone(boolean monotone) {
        isMonotone = monotone;
    }
}
