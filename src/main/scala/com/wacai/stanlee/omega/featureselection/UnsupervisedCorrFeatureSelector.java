package com.wacai.stanlee.omega.featureselection;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.ArrayList;
import java.util.List;

/**
 * @author manshahua@wacai.com
 * @date 2018/1/8 下午5:12
 */

/**
 * 通过特征两两进行计算相关系数，相关系数大的肯定是可以去掉的
 */
public class UnsupervisedCorrFeatureSelector {
    public List<String> featureSelect(Dataset<Row> samples, double threshold, List<String> featureNames)
    {
        int length = featureNames.size();
        List<String> selectedFeatureNames = new ArrayList<>();
        selectedFeatureNames.add(featureNames.get(0));
        for(int i=1; i<length; i++)
        {
            String beforeFeatureName = featureNames.get(i);
            int selectedLength = selectedFeatureNames.size();
            for(int j=0; j<selectedLength; j++)
            {
                String afterFeatureName = selectedFeatureNames.get(j);
                double corr = samples.stat().corr(beforeFeatureName, afterFeatureName);
                System.out.println("特征"+beforeFeatureName+"和特征"+afterFeatureName+"的相关系数为："+corr);
                if(Math.abs(corr)>threshold)
                {
                    continue;
                }
            }
            selectedFeatureNames.add(beforeFeatureName);
        }
        return selectedFeatureNames;
    }
    public static void main(String[] args)
    {

    }
}
