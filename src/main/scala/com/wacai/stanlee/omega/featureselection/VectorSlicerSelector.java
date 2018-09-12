package com.wacai.stanlee.omega.featureselection;
import java.util.List;

import com.google.common.collect.Lists;

import org.apache.spark.ml.attribute.Attribute;
import org.apache.spark.ml.attribute.AttributeGroup;
import org.apache.spark.ml.attribute.NumericAttribute;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorSlicer;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
/**
 * @author manshahua@wacai.com
 * @date 2017/12/22 上午10:16
 */
public class VectorSlicerSelector {
    public static Dataset<Row> featureSelectionByVectorSlicer(Dataset<Row> samples,  String[] selectedFeatureNames)
    {
        VectorAssembler vectorAssembler = new VectorAssembler().setInputCols(selectedFeatureNames).setOutputCol("selectedFeatureNames");
        Dataset<Row> transformedSamples = vectorAssembler.transform(samples);
        System.out.println("经过vectore assemble之后的： ");
        transformedSamples.show();
        Dataset<Row> slicerSamples = transformedSamples.select("selectedFeatureNames", "label");
        return slicerSamples;
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("spark test")
                .master("local[2]")
                .enableHiveSupport()
                .getOrCreate();

        Dataset<Row> data = spark.read().option("header", "true").csv("/Users/duty/Documents/iris.txt");
        System.out.println("原始data： ");
        data.show();
        data.printSchema();
        Dataset<Row> newData = data.selectExpr("sepal_length","sepal_width","petal_length","petal_width","case when species = \'setosa\' then 1 \n" +
                "when species = \'versicolor\' then 2\n" +
                "when species = \'virginica\' then 3\n" +
                "end as label");
        newData = newData.withColumn("userless", functions.lit(1.0));
        newData = newData.withColumn("sepal_length",functions.col("sepal_length").cast("double"));
        newData = newData.withColumn("sepal_width",functions.col("sepal_width").cast("double"));
        newData = newData.withColumn("petal_length",functions.col("petal_length").cast("double"));
        newData = newData.withColumn("petal_width",functions.col("petal_width").cast("double"));
        System.out.println("新增一列之后的data： ");
        newData.printSchema();
        newData.show();
        String[] selectedFeatureNames = {"sepal_length","sepal_width"};
        Dataset<Row> slicerSamples = featureSelectionByVectorSlicer(newData, selectedFeatureNames);
        slicerSamples.show();

    }
}
