package com.wacai.stanlee.omega.util;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.List;
import java.util.Map;

/**
 * @author manshahua@wacai.com
 * @date 2018/1/11 下午4:50
 */
public class InformationGain {
    private Dataset<Row> samples;
    private String labelName;
    private List<String> featureNames;
    public List<Integer> getEveryClassSampleNum()
    {
        long totalNum = samples.count();
        Dataset<Row> labelCountsData = samples.groupBy(functions.col(labelName)).count();
        JavaRDD<Map<String, Long>> labelCountMap = labelCountsData.toJavaRDD().map(new Function<Row, Map<String,Long>>() {
            @Override
            public Map<String, Long> call(Row row) throws Exception {
                return null;
            }
        });
        return null;
    }
    public double calcInformationGain(String featureName, List<Double> valueIntervals)
    {
        return 0;
    }

}
