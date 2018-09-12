package com.wacai.stanlee.omega.featureselection;

/**
 * @author manshahua@wacai.com
 * @date 2018/1/8 下午7:36
 */

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.MinMaxScalerModel;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import javax.xml.crypto.Data;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

/**
 * 通过计算每个特征的方差，如果一个特征不发散，例如方差接近于0，也就是说样本在这个特征上基本上没有差异，这个特征对于样本的区分并没有什么用
 */
public class UnsupervisedVarianceFeatureSelector implements Serializable{
    public List<String> featureSelect(Dataset<Row> samples, List<String> featureNames)
    {
        SparkSession sparkSession = samples.sparkSession();
        String scaledOutputColumnName = "scaledfeatures";
        String assembleOutputColumnName = "features";
        int length = featureNames.size();
        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(featureNames.toArray(new String[0]))
                .setOutputCol(assembleOutputColumnName);
        Dataset<Row> assembleData = vectorAssembler.transform(samples);

        MinMaxScaler minMaxScaler = new MinMaxScaler()
                .setInputCol(assembleOutputColumnName)
                .setOutputCol(scaledOutputColumnName);
        MinMaxScalerModel minMaxScalerModel = minMaxScaler.fit(assembleData);
        Dataset<Row> scaledData = minMaxScalerModel.transform(assembleData);
        scaledData = scaledData.select(scaledOutputColumnName);
        scaledData.show();
        //最终出来的dataset包含了原始字段名，feature，和scaledfeature
        //装换为rowrdd，然后对每一行进行操作
        JavaRDD<Row> rowRdd = scaledData.toJavaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                DenseVector denseVector = (DenseVector) row.get(0);
                double[] doubles = denseVector.toArray();
                Double[] values = new Double[doubles.length];
                for(int i=0; i<doubles.length; i++)
                {
                    values[i] = Double.valueOf(doubles[i]);
                }
//                List<Double> rowData = new ArrayList<>();
//                for(int i=0; i<doubles.length; i++)
//                {
//                    rowData.add(doubles[i]);
//                }
                return RowFactory.create(values);
            }
        });

        List<StructField> fields = new ArrayList<StructField>();
        for(int i=0; i<length; i++)
        {
            StructField field = DataTypes.createStructField(featureNames.get(i), DataTypes.DoubleType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> df = sparkSession.createDataFrame(rowRdd, schema);
        df.show();
        List<String> selectedFeatures = new ArrayList<>();
        //生成sql
        df.createOrReplaceTempView("scaleddata");
        String avgStdColumnSqls = "";
        for(int i=0; i<length; i++)
        {
            String avgSql = "avg("+featureNames.get(i)+") as avg_"+featureNames.get(i);
            String stdSql = "std("+featureNames.get(i)+") as std_"+featureNames.get(i);
            avgStdColumnSqls = avgStdColumnSqls + avgSql+",\n"+stdSql+(i==length-1?"\n":",\n");
        }
        Dataset<Row> avgStdData = sparkSession.sql("select \n"+avgStdColumnSqls+"from scaleddata");
        avgStdData.show();
        //这个速度太慢了，还是spark sql比较快点
        Row avgStdRowData = avgStdData.first();
        for(int i=0; i<length; i++)
        {
//            Dataset<Row> avgStdData = df.agg(functions.avg(featureNames.get(i)), functions.stddev(featureNames.get(i)));
            String featureName = featureNames.get(i);
            Double avg = avgStdRowData.getAs("avg_"+featureName);
            Double std = avgStdRowData.getAs("std_"+featureName);
            System.out.println("avg: " + avg + "std: " + std);
            if(std>0.001)
                selectedFeatures.add(featureNames.get(i));
        }

        return selectedFeatures;
    }
}
