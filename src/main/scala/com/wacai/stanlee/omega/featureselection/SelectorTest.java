package com.wacai.stanlee.omega.featureselection;

import org.apache.spark.sql.*;

import java.io.File;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.Writer;
import java.util.*;

/**
 * @author manshahua@wacai.com
 * @date 2017/12/27 上午11:58
 */
public class SelectorTest implements Serializable {
    public static void main(String[] args)
    {
        SparkSession spark = SparkSession
                .builder()
                .appName("spark test")
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();
        FeatureTable featureTable = FeatureTable.fromFile("featureTable.json");
        featureTable.setTableName("ads_model_repeat_buyer_prediction_details2_final_result4");
//        featureTable.setTableName("ads_model_repeat_buyer_prediction_details2_result");
//        featureTable.setTableName("iris_data_binary");
//        featureTable.setSelectedFeatureNum(featureNum);
//        featureTable.setSelectedFeatureThreshold(threshold);
        FeatureSelectorParam featureSelectorParam = new FeatureSelectorParam();
//        featureSelectorParam.init(featureTable, spark);
//        String[] featureNames = {"sepal_length","sepal_width","petal_length","petal_width","userless","double_sepal_length"};
//        featureSelectorParam.getSamples().show();
//        Dataset<Row> newData = featureSelectorParam.getSamples().withColumn("double_sepal_length",functions.expr("2*sepal_length"));
//        newData.show();
        //无监督的相关系数选择
        Dataset<Row> tmpSet = spark.read().table("ads_model_repeat_buyer_prediction_details2_final_result4");
        tmpSet.show(10);
        String[] featureNames = tmpSet.columns();
        List<String> newFeatureNames = new ArrayList<>();
        for(int i=0; i<featureNames.length; i++)
        {
            if(!"label".equals(featureNames[i])&&!"user_id".equals(featureNames[i])&&!"merchant_id".equals(featureNames[i]))
                newFeatureNames.add(featureNames[i]);
        }
//        UnsupervisedCorrFeatureSelector unsupervisedCorrFeatureSelection = new UnsupervisedCorrFeatureSelector();
//        unsupervisedCorrFeatureSelection.featureSelect(tmpSet,0.9, newFeatureNames);
//        System.out.println("dddd");
//        无监督的方差统计法
        UnsupervisedVarianceFeatureSelector unsupervisedVarianceFeatureSelector = new UnsupervisedVarianceFeatureSelector();
        unsupervisedVarianceFeatureSelector.featureSelect(tmpSet, newFeatureNames);
        System.out.println("ddd");
//        List<String> selectedFeatures = new ArrayList<>();
//        Map<String, Double> totalFeaturesImportane = new HashMap<>();
        //测试随机森林
//        RandomForestSelector randomForestSelector = new RandomForestSelector();
//        selectedFeatures = randomForestSelector.featureSelect(featureSelectorParam);
//        totalFeaturesImportane = randomForestSelector.getTotalFeaturesImportance();
//        System.out.println(Arrays.toString(selectedFeatures.toArray()));
//        //测试相关系数法
//        PearsonCorrSelector pearsonCorrSelector = new PearsonCorrSelector();
//        selectedFeatures = pearsonCorrSelector.featureSelect(featureSelectorParam);
//        totalFeaturesImportane = pearsonCorrSelector.getTotalFeaturesImportance();
//        System.out.println(Arrays.toString(selectedFeatures.toArray()));
        //测试ks
//        KsSelector ksSelector = new KsSelector();
//        selectedFeatures = ksSelector.featureSelect(featureSelectorParam);
//        totalFeaturesImportane = ksSelector.getTotalFeaturesImportance();
//        System.out.println(Arrays.toString(selectedFeatures.toArray()));
        //测试IV
//        IvSelector ivSelector = new IvSelector();
//        selectedFeatures = ivSelector.featureSelect(featureSelectorParam);
//        System.out.println(Arrays.toString(selectedFeatures.toArray()));
//        Map<String, IvIntervalStat> stats = ivSelector.getIvIntervalStats();
//        Writer writer = null;
//        try{
//            writer = new PrintWriter(new File("iv_interval_info"));
//            for(Map.Entry<String, IvIntervalStat> entry: stats.entrySet())
//            {
//                String key = entry.getKey();
//                IvIntervalStat ivIntervalStat = entry.getValue();
//                Map<String, String> intervalInfos = ivIntervalStat.getIntervalInfos();
//                boolean isMonotone = ivIntervalStat.isMonotone();
//                for(Map.Entry<String, String> entry1:intervalInfos.entrySet())
//                {
//                    writer.write(key+","+entry1.getKey()+","+entry1.getValue()+","+isMonotone+"\n");
//                }
//            }
//            writer.close();
//        }catch (Exception e)
//        {
//            e.printStackTrace();
//        }
        System.out.println("ddddd");
        //测试卡方校验
//        ChiSquareSelector chiSquareSelector = new ChiSquareSelector();
//        selectedFeatures = chiSquareSelector.featureSelect(featureSelectorParam);
//        System.out.println(Arrays.toString(selectedFeatures.toArray()));

    }
}
