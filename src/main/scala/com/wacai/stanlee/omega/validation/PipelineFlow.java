package com.wacai.stanlee.omega.validation;


import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConversions.*;

import java.util.List;

/**
 * @author manshahua@wacai.com
 * @date 2017/12/25 下午8:54
 */
public class PipelineFlow {
    public static void main(String[] args)
    {
        SparkSession sparkSession = SparkSession.builder()
                .appName("classification test")
                .master("local[2]")
                .enableHiveSupport()
                .getOrCreate();
        DataFrameReader dataFrameReader = new DataFrameReader(sparkSession);
        Dataset<Row> irisData = dataFrameReader.table("feature_selected_table");
//        irisData.printSchema();
        Dataset<Row>[] irisDatas = irisData.randomSplit(new double[]{0.7,0.3});
        Dataset<Row> irisTrainData  = irisDatas[0];
        Dataset<Row> irisTestData = irisDatas[1];

        irisData.show(10);
        irisTrainData.show(10);
        irisTestData.show(10);

        String[] featureNames = "c_action_type_2_brand_id, cd_action_type_2_item_id, c_action_type_2_item_id, c_action_type_2_cat_id, cd_action_type_0_cat_id, cd_time_stamp_3_cat_id, cd_action_type_0_time_stamp_3_cat_id, cd_action_type_0_time_stamp_2_brand_id, cd_time_stamp_2_brand_id, cd_action_type_0_item_id, c_action_type_0_item_id, c_action_type_0_cat_id, c_action_type_0_brand_id, cd_action_type_2_cat_id, cd_time_stamp_3_item_id, cd_action_type_0_time_stamp_3_item_id, cd_action_type_0_time_stamp_2_cat_id, cd_time_stamp_2_cat_id, c_time_stamp_3_brand_id, c_time_stamp_3_cat_id, c_time_stamp_3_item_id, cd_action_type_3_item_id, c_action_type_3_item_id, c_action_type_3_brand_id, c_action_type_3_cat_id, cd_action_type_3_time_stamp_3_item_id, cd_time_stamp_1_brand_id, cd_time_stamp_1_cat_id, c_action_type_3_time_stamp_3_brand_id, c_action_type_3_time_stamp_3_item_id, c_action_type_3_time_stamp_3_cat_id, cd_action_type_0_time_stamp_1_brand_id, cd_action_type_0_time_stamp_1_cat_id, c_action_type_0_time_stamp_3_brand_id, c_action_type_0_time_stamp_3_cat_id, c_action_type_0_time_stamp_3_item_id, cd_time_stamp_2_item_id, cd_action_type_0_time_stamp_2_item_id, cd_action_type_3_time_stamp_2_cat_id, cd_action_type_3_time_stamp_2_brand_id, cd_action_type_0_time_stamp_1_item_id, cd_time_stamp_1_item_id, c_time_stamp_2_cat_id, c_action_type_0_time_stamp_2_brand_id, c_action_type_0_time_stamp_2_cat_id, c_time_stamp_2_item_id, c_time_stamp_2_brand_id, c_action_type_0_time_stamp_2_item_id, c_action_type_0_time_stamp_1_item_id, c_action_type_0_time_stamp_1_cat_id".split(",");

        VectorAssembler vectorAssembler = new VectorAssembler().setInputCols(featureNames).setOutputCol("features");
//        irisData = vectorAssembler.transform(irisData);

        RandomForestClassifier randomForestClassifier = new RandomForestClassifier();
        randomForestClassifier.setFeaturesCol("features").setLabelCol("label");
        System.out.println(randomForestClassifier.explainParams());

        //ml过程就像一个流水线，只需要定义好每个中间过程需要做的事情，就然数据从源头开始沿着流水线流淌就行了
        PipelineStage[] pipelineStages = {vectorAssembler, randomForestClassifier};
        Pipeline pipeline = new Pipeline().setStages(pipelineStages);
        PipelineModel pipelineModel = pipeline.fit(irisTrainData);
//        pipelineModel.
//        Dataset<Row> output = pipelineModel.transform(irisTestData);
//        output.show();
//        RandomForestClassificationModel randomForestClassificationModel = randomForestClassifier.fit(irisTrainData);
//        Dataset<Row> testData = randomForestClassificationModel.transform(irisTestData);
//        testData.show();
        randomForestClassifier.explainParams();
        System.out.println();

    }
}
