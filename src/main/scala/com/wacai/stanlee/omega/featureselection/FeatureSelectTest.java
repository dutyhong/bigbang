package com.wacai.stanlee.omega.featureselection;

import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author manshahua@wacai.com
 * @date 2017/12/25 下午3:50
 */
public class FeatureSelectTest {
    public List<String> featureSelect(FeatureSelectorParam featureSelectorParam)
    {
        String selectMethod = featureSelectorParam.getSelectMethod();
        List<String> selectedFeatureNames = new ArrayList<>();
        if("chi".equals(selectMethod))
        {
            ChiSquareSelector chiSquareSelector = new ChiSquareSelector();
            selectedFeatureNames = chiSquareSelector.featureSelect(featureSelectorParam);
        }else if("rf".equals(selectMethod))
        {
            RandomForestSelector randomForestSelector = new RandomForestSelector();
            selectedFeatureNames = randomForestSelector.featureSelect(featureSelectorParam);
        }else if("corr".equals(selectMethod))
        {
            PearsonCorrSelector pearsonCorrSelector = new PearsonCorrSelector();
            selectedFeatureNames = pearsonCorrSelector.featureSelect(featureSelectorParam);
        }
        return selectedFeatureNames;
    }
    public static void main(String[] args)
    {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[2]")
                .enableHiveSupport()
                .appName("feature select test")
                .getOrCreate();
        FeatureSelectTest featureSelectTest = new FeatureSelectTest();
        FeatureTable featureTable = FeatureTable.fromFile("featureTable.json");
        FeatureSelectorParam featureSelectorParam = new FeatureSelectorParam();
        featureSelectorParam.init(featureTable, sparkSession);
        featureSelectorParam.getSamples().printSchema();
        List<String> selectedFeatureNames = featureSelectTest.featureSelect(featureSelectorParam);
        System.out.println(Arrays.toString(selectedFeatureNames.toArray()));

    }
}
