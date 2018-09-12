package com.wacai.stanlee.omega.featurebin;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author manshahua@wacai.com
 * @date 2018/2/26 下午4:31
 */
public class ChiMerge {
    /**
     *
     * @param singleFeatureSamples 每个特征的数据
     * @param originalBins 等频初始的分箱
     * @param overAllRate 各类别的样本总体的比例.暂时只有坏样本占比
     * @param maxInterval 最大分箱的个数
     * @return 卡方分箱后的区间值
     */
    public List<Double> splitBin(Dataset<Row> singleFeatureSamples, List<Double> originalBins, double overAllRate,
                             int maxInterval, String featureName, String labelName)
    {
        //计算每个相邻的两个区间的卡方值，将具有最小卡方值的区间进行合并，卡方计算公式：(A-E)*(A-E)/E
        List<Row> rows = singleFeatureSamples.collectAsList();
        int originalSize = originalBins.size();
        int newSize = originalSize;
        List<Double> newBins = new ArrayList<>();
        for(int i=0; i<originalSize; i++)
        {
            newBins.add(originalBins.get(i));
        }
        while(newSize>(maxInterval+1)||!isBadRateMonotone(singleFeatureSamples,newBins,featureName,labelName))
        {
            double minValue = 10000000;
            int minIndex = 0;
            for(int i=0; i<newSize-2; i++)
            {
                double start = newBins.get(i);
                double middle = newBins.get(i+1);
                double end = newBins.get(i+2);
                List<Double> neighborBins = new ArrayList<>();
                neighborBins.add(start);
                neighborBins.add(middle);
                neighborBins.add(end);
                //计算相邻区间的卡方值
                double chi2Value = chi(rows, neighborBins, overAllRate, featureName, labelName);
                if(chi2Value<minValue)
                {
                    minValue = chi2Value;
                    minIndex = i+1;
                }
//                chi2ValueList.add(chi2Value);
            }
            newBins.remove(minIndex);
            newSize = newBins.size();
        }
        return newBins;
    }
    //计算相邻分箱的卡方值
    private double chi(List<Row> rows, List<Double> neighborBins, double overAllRate, String featureName, String labelName)
    {
        //统计相邻区间内的样本数
        int intervalNum = neighborBins.size();
        int[] everyIntervalGoodNum = new int[intervalNum-1];
        int[] everyIntervalBadNum = new int[intervalNum-1];
        for(int i=0; i<intervalNum-1; i++) {
            double startValue = neighborBins.get(i);
            double endValue = neighborBins.get(i + 1);
            for (Row row : rows) {
                Object o = row.getAs(featureName);
                double featureValue = o instanceof Long ? (double) (long) o : (double) o;
                long labelValue = row.getAs(labelName);
                if (0 == labelValue && featureValue >= startValue && featureValue < endValue) {
                    everyIntervalGoodNum[i] = everyIntervalGoodNum[i] + 1;
                } else if (1 == labelValue && featureValue >= startValue && featureValue < endValue) {
                    everyIntervalBadNum[i] = everyIntervalBadNum[i] + 1;
                }
            }
        }
        //计算卡方值
        int[] everyIntervalExpectedBadNum = new int[intervalNum-1];
        double chi = 0;
        for(int i=0; i<intervalNum-1; i++)
        {
            everyIntervalExpectedBadNum[i] = (int) ((everyIntervalGoodNum[i]+everyIntervalBadNum[i])*overAllRate);
            chi = chi + Math.pow(everyIntervalExpectedBadNum[i]-everyIntervalBadNum[i], 2)/everyIntervalExpectedBadNum[i];
        }
        return chi;
    }
    //根据等频分隔初始区间
    public List<Double> getIntervalArrayEqualFrequency(Dataset<Row> singleFeatureSamples, String featureName, int intervalNum)
    {
        long totalNum = singleFeatureSamples.count();
        if(totalNum<intervalNum)
        {
            System.err.println("样本数比区间数小！！！！！！！！！");
            return null;
        }
        List<Row> rows = singleFeatureSamples.collectAsList();
        //将所有的特征值放入list
        List<Double> featureValues = new ArrayList<>();
        for(Row row: rows)
        {
            double value = 0;
            if(row.getAs(featureName) instanceof Double)
            {
                value = row.getAs(featureName);
            }else
            {
                value = (double)(long)row.getAs(featureName);
            }
            featureValues.add(value);
        }
        Collections.sort(featureValues);
        //取出每个rank对应的数值
        int everyIntervalSampleCnt = (int) (totalNum/intervalNum);

        int valueIndices[] = new int[intervalNum];
        valueIndices[0] = 1;

        for(int i=1; i<intervalNum-1; i++)
        {
            valueIndices[i] = everyIntervalSampleCnt*i;
        }
        valueIndices[intervalNum-1] = (int)totalNum;
        List<Double> intervalArrayValue = new ArrayList<>();
        for(int i=0; i<intervalNum; i++)
        {
            intervalArrayValue.add(featureValues.get(valueIndices[i]-1));
        }
        //由于有时候按照等量等分会有某个取值对应的样本量很大的情况，所以只取不同值
        List<Double> intervalArrayValueNew = new ArrayList<>();
        for(int i=0; i<intervalArrayValue.size(); i++)
        {
            if(intervalArrayValueNew.contains(intervalArrayValue.get(i)))
            {
                continue;
            }
            intervalArrayValueNew.add(intervalArrayValue.get(i));
        }
        return intervalArrayValueNew;
    }
    //判断新的分箱是否单调，不单调没有业务含义
    private boolean isBadRateMonotone(Dataset<Row> singleFeatureSamples, List<Double> newBins, String featureName, String labelName)
    {
        List<Row> rows = singleFeatureSamples.collectAsList();
        int intervalNum = newBins.size()-1;
        //要10个箱就要11个值
        int[] everyIntervalGoodNum = new int[intervalNum];
        int[] everyIntervalBadNum = new int[intervalNum];
        double[] badRates = new double[intervalNum];
        for(int i=0; i<intervalNum; i++) {
            double startValue = newBins.get(i);
            double endValue = newBins.get(i + 1);
            for (Row row : rows) {
                Object o = row.getAs(featureName);
                double featureValue = o instanceof Long ? (double) (long) o : (double) o;
                long labelValue = row.getAs(labelName);
                if (0 == labelValue && featureValue >= startValue && featureValue < endValue) {
                    everyIntervalGoodNum[i] = everyIntervalGoodNum[i] + 1;
                } else if (1 == labelValue && featureValue >= startValue && featureValue < endValue) {
                    everyIntervalBadNum[i] = everyIntervalBadNum[i] + 1;
                }
            }
        }
        for(int i=0; i<intervalNum; i++)
        {
            badRates[i] = (double) everyIntervalBadNum[i]/(everyIntervalBadNum[i]+everyIntervalGoodNum[i]);
        }
        for(int i=1; i<intervalNum-1; i++)
        {
            if( (badRates[i]>badRates[i-1]&&badRates[i]>badRates[i+1]) || (badRates[i]<badRates[i-1]&&badRates[i]<badRates[i+1]))
                return false;
        }
        return true;
    }
    public static void main(String[] args)
    {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("bintest")
                .enableHiveSupport()
                .getOrCreate();
//        Dataset<Row> data = sparkSession.read().format("orc").load("hdfs://10.1.169.1:9000/data/dw/raw/modeling_db.db/xf_wht_calls_6m_samples_small");
        Dataset<Row> data = sparkSession.read().option("header","true").csv("/Users/duty/PycharmProjects/algorithms/new_application.csv");
        data.show(10);
        String[] featureNames = data.columns();
        Dataset<Row> singleFeatureSamples = data.select("y","int_rate_clean");

        String featureName = "int_rate_clean";
        String labelName = "y";
        singleFeatureSamples = singleFeatureSamples.withColumn(labelName, functions.col(labelName).cast("long"));
        singleFeatureSamples = singleFeatureSamples.withColumn(featureName, functions.col(featureName).cast("double"));

        singleFeatureSamples.printSchema();
        ChiMerge chiMerge = new ChiMerge();
        List<Double> originalBins = chiMerge.getIntervalArrayEqualFrequency(singleFeatureSamples, featureName, 100);
        List<Double> newBins = chiMerge.splitBin(singleFeatureSamples,originalBins,0.1,10,featureName,labelName);

        System.out.println("dd");
    }
}
