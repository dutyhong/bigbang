package com.wacai.stanlee.omega.featureselection;

import com.wacai.stanlee.omega.util.MapSort;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author manshahua@wacai.com
 * @date 2017/12/22 下午3:19
 */
public class PearsonCorrSelector implements FeatureSelector {
    private Map<String, Double> totalFeaturesImportance = new HashMap<>();

    public Map<String, Double> getTotalFeaturesImportance() {
        return totalFeaturesImportance;
    }

    public void setTotalFeaturesImportance(Map<String, Double> totalFeaturesImportance) {
        this.totalFeaturesImportance = totalFeaturesImportance;
    }
    private Map<String, Double> corrPositiveOrNegative = new HashMap<>();

    public Map<String, Double> getCorrPositiveOrNegative() {
        return corrPositiveOrNegative;
    }

    public void setCorrPositiveOrNegative(Map<String, Double> corrPositiveOrNegative) {
        this.corrPositiveOrNegative = corrPositiveOrNegative;
    }

    //皮尔森相关系数：取值范围为【-1，1】 负值表示负相关，正值表示正相关，绝对值越大表示相关性越强
    @Override
    public  List<String> featureSelect(FeatureSelectorParam featureSelectorParam)
    {
        Dataset<Row> samples = featureSelectorParam.getSamples();
        String[] featureNames = featureSelectorParam.getFeatureColumnNames();
        double threshhold = featureSelectorParam.getSelectedFeatureThreshold();
        String labelName = featureSelectorParam.getLabelName();
        long selectedFeatureNum = featureSelectorParam.getSelectedFeatureNum();
        samples = samples.withColumn(labelName, functions.col(labelName).cast("long"));

        List<Column> cols = new ArrayList<>();
        for(String featureName :featureNames)
        {
            double corr = samples.stat().corr(labelName, featureName);
            totalFeaturesImportance.put(featureName, Math.abs(corr));
            corrPositiveOrNegative.put(featureName, corr);
//            cols.add(functions.corr(labelName, featureName).alias(featureName));
        }
//        Dataset<Row> featureCorr = samples.agg(functions.first(labelName), (scala.collection.Seq)scala.collection.JavaConversions.asScalaBuffer(cols));
//        featureCorr.
//        Row[] corrValueRows = (Row[]) featureCorr.collect();//只有一行

//        String[] featureColumnNames = featureCorr.columns();

        List<String> selectedFeatures = new ArrayList<>();
        //第一各元素是label
//        for(int i=1; i<featureColumnNames.length; i++)
//        {
//            //有可能相关系数计算出来是null因为他有相除会有除零
//            if(corrValueRows[0].get(i) instanceof Double){
//                totalFeaturesImportance.put(featureColumnNames[i], Math.abs(corrValueRows[0].getDouble(i)));
//            }
//        }
        Map<String, Double> result = MapSort.sortByValueDesc(totalFeaturesImportance);
        if(selectedFeatureNum>0)
        {
            int ind = 0;
            for(Map.Entry<String, Double> entry: result.entrySet())
            {
                ind++;
                if(ind>selectedFeatureNum)
                    break;
                selectedFeatures.add(entry.getKey());
            }
        }else{
            for(Map.Entry<String, Double> entry: result.entrySet())
            {
                if(entry.getValue()<threshhold)
                    break;
                selectedFeatures.add(entry.getKey());
            }
        }
        return selectedFeatures;
    }

    public static void main(String[] args)
    {

    }
}
