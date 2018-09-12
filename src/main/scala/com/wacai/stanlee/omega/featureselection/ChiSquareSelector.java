package com.wacai.stanlee.omega.featureselection;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author manshahua@wacai.com
 * @date 2017/12/20 下午7:13
 */
public class ChiSquareSelector implements FeatureSelector{
    @Override
    public List<String> featureSelect(FeatureSelectorParam featureSelectorParam) {
        Dataset<Row> feature = featureSelectorParam.getSamples();
        String labelName = featureSelectorParam.getLabelName();
        feature = feature.withColumn(labelName, functions.col(labelName).cast("long"));
        feature.printSchema();
        Double threshHoldFpr = featureSelectorParam.getSelectedFeatureThreshold();
        Integer featureNum = featureSelectorParam.getSelectedFeatureNum();
        String[] featureNames = featureSelectorParam.getFeatureColumnNames();
        VectorAssembler va = new VectorAssembler()
                .setInputCols(featureNames).setOutputCol("features");
        feature = va.transform(feature);


        Dataset<Row> newFeature = feature.select("features", labelName);

        //.unpersist();

        feature.unpersist();
        // scaler
        StandardScaler scaler = new StandardScaler()
                .setInputCol("features")
                .setOutputCol("scaledFeatures")
                .setWithStd(true)
                .setWithMean(false);
        Dataset<Row> scalerFeature = scaler.fit(newFeature).transform(newFeature);
        //
        ChiSqSelector selector = null;
        if (featureNum <= 0) {
            selector = new ChiSqSelector()
                    .setFpr(threshHoldFpr)
//            .setPercentile(threshholdfpr)
                    .setFeaturesCol("scaledFeatures")
                    .setLabelCol(labelName)
                    .setOutputCol("selectedFeatures");
        } else {
            selector = new ChiSqSelector()
                    .setNumTopFeatures(featureNum)
                    .setFeaturesCol("scaledFeatures")
                    .setLabelCol(labelName)
                    .setOutputCol("selectedFeatures");
        }
        ChiSqSelectorModel selectorModel = selector.fit(scalerFeature);
        int[] indices = selectorModel.selectedFeatures();
        List<String> tempList = new ArrayList<>();
        for (int i = 0; i < indices.length; i++) {
            tempList.add(featureNames[indices[i]]);
        }

        return tempList;
    }



}