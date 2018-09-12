package com.wacai.stanlee.omega.featureselection;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.*;

/**
 * @author manshahua@wacai.com
 * @date 2018/2/5 下午4:39
 */
public class InformationEntropy {
    public static double computeIntervalEntropy(Dataset<Row> singleFeatureSample, String featureName, String labelName, List<Double> intervalValues)
    {
        List<Row> rows = singleFeatureSample.collectAsList();
        int size = rows.size();
        int intervalSize = intervalValues.size();
        //存放每个区间对应的样本数
        int[] intervalNums = new int[intervalSize-1];
        //存放每个label对应的样本数
        Map<String, Integer> labelNums = new HashMap<>();
        for(int i=0; i<intervalSize-1; i++)
        {
            double beforeValue = intervalValues.get(i);
            double afterValue = intervalValues.get(i+1);
            for(int j=0; j<size; j++)
            {
                Row row = rows.get(j);
                Object o = row.getAs(featureName);
                double featureValue = o instanceof Double? (double) o:(double)(long)o;
                if(featureValue>=beforeValue && featureValue<afterValue)
                {
                    intervalNums[i] = intervalNums[i]+1;
                }
                Object o1 = row.getAs(labelName);
                String labelValue = String.valueOf(o1);
                //统计label得个数放入map
                if(labelNums.containsKey(labelValue)){
                    int value = labelNums.get(labelValue);
                    value = value + 1;
                    labelNums.put(labelValue, value);
                }else{
                    labelNums.put(labelValue, 1);
                }
            }

        }
        //统计每个区间内属于各个label的样本数
        Set<String> labelNames = labelNums.keySet();
        List<Map<String, Integer>> intervalCategoryMap = new ArrayList<>();
        for(int i=0; i<intervalSize-1; i++)
        {
            double beforeValue = intervalValues.get(i);
            double afterValue = intervalValues.get(i + 1);
            Map<String, Integer> labelMap = new HashMap<>();
            for(Row row:rows)
            {
                Object o = row.getAs(featureName);
                double featureValue = o instanceof Double? (double) o:(double)(long)o;
                if(featureValue>=beforeValue && featureValue<afterValue)
                {
                    Object o1 = row.getAs(labelName);
                    String labelValue = String.valueOf(o1);
                    if(labelMap.containsKey(labelValue)){
                        int value = labelMap.get(labelValue);
                        value = value + 1;
                        labelMap.put(labelValue, value);
                    }else{
                        labelMap.put(labelValue,1);
                    }
                }
            }
            intervalCategoryMap.add(labelMap);
        }
        //开始计算条件熵 -Da/D(Da0/Da*log(Da0/Da)+Da1/Da*log(Da1/Da))
        double conditionEntropy = 0;
        for(int i=0; i<intervalNums.length; i++)
        {
            double da = (double)intervalNums[i];
            double categoryProb = da/size;
            Map<String, Integer> tmpMap = intervalCategoryMap.get(i);
            double categoryEntropy = 0;
            for(Map.Entry<String, Integer> entry: tmpMap.entrySet())
            {
                int value = entry.getValue();
                categoryEntropy = categoryEntropy + value/da*Math.log(value/da);
            }
            conditionEntropy = conditionEntropy -categoryProb*categoryEntropy;
        }

        return conditionEntropy;
    }
}
