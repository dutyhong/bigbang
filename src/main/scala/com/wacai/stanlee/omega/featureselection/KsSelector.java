package com.wacai.stanlee.omega.featureselection;

import com.wacai.stanlee.omega.util.MapSort;
import org.apache.spark.sql.*;

import java.util.*;

/**
 * @author manshahua@wacai.com
 * @date 2017/12/26 下午4:57
 */
public class KsSelector implements FeatureSelector{
    private int ksInterval = 10;
    private List<Double> totalKsValue = new ArrayList<>();
    private Map<String, Double> totalFeaturesImportance = new HashMap<>();
    @Override
    public List<String> featureSelect(FeatureSelectorParam featureSelectorParam)
    {
        Dataset<Row> samples = featureSelectorParam.getSamples();
        String labelName = featureSelectorParam.getLabelName();
        samples = samples.withColumn(labelName, functions.col(labelName).cast("long"));
        String[] featureColumnNames = featureSelectorParam.getFeatureColumnNames();
        long selectedFeatureNum = featureSelectorParam.getSelectedFeatureNum();
        double selectedFeatureThreshold = featureSelectorParam.getSelectedFeatureThreshold();
        long length = featureColumnNames.length;
        for(int i=0; i<length; i++)
        {
            Dataset<Row> singleFeatureSamples = samples.select(featureColumnNames[i], labelName);
            double start = System.currentTimeMillis();
            double singleFeatureKsValue = singleFeatureKsValue(singleFeatureSamples, featureColumnNames[i], labelName, ksInterval);
            double end = System.currentTimeMillis();
            System.out.println("每个时间为："+(end-start)/(1000));
            totalKsValue.add(singleFeatureKsValue);
            totalFeaturesImportance.put(featureColumnNames[i], singleFeatureKsValue);
            System.out.println("特征列："+featureColumnNames[i]+"ks值 ： "+singleFeatureKsValue);
        }
        List<String> selectedFeatureNames = new ArrayList<>();
        //根据输入的特征选择数和阈值选出符合条件的特征
        Map<String, Double> sortedFeatureKsValues = MapSort.sortByValueDesc(totalFeaturesImportance);
        if(selectedFeatureNum>0)
        {
            int ind = 0;
            for(Map.Entry<String, Double> entry: sortedFeatureKsValues.entrySet())
            {
                ind++;
                selectedFeatureNames.add(entry.getKey());
                if(ind>=selectedFeatureNum)
                    break;

            }
        }else{
            for(Map.Entry<String, Double> entry: sortedFeatureKsValues.entrySet())
            {
                if(selectedFeatureThreshold>entry.getValue())
                    break;
                selectedFeatureNames.add(entry.getKey());
            }
        }
        return selectedFeatureNames;
    }
    public double singleFeatureKsValue(Dataset<Row> singleFeatureSamples, String featureName, String labelName, int ksInterval)
    {
        List<Row> rows = singleFeatureSamples.collectAsList();
        List<Row> labels = singleFeatureSamples.select(labelName).distinct().collectAsList();
        //取出label的值 0，1
        if(labels.size()>=3)
        {
            System.out.println("Ks selector error the num of classes bigger than 2 ,ks only used for binary classification");
        }
        //好的样本标签为1，坏的样本标签为0
        long goodSampleNum = 0;//singleFeatureSamples.where(labelName+"=1").count();
        long badSampleNum = 0;//singleFeatureSamples.where(labelName+"=0").count();
        double maxValue = 0d;//maxValueDataset.head().get(0) instanceof Long?(long)maxValueDataset.head().get(0):(double)maxValueDataset.head().get(0);
        double minValue = 0d;//minValueDataset.head().get(0) instanceof Long?(long)minValueDataset.head().get(0):(double)minValueDataset.head().get(0);

        //统计总的正负样本和最大最小值
        for(Row row: rows)
        {
            long labelValue = row.getAs(labelName);
            if(0==labelValue)
                goodSampleNum = goodSampleNum + 1;
            else if(1==labelValue)
                badSampleNum =badSampleNum + 1;
            Object o = row.getAs(featureName);
            double tmpValue = o instanceof Double?(double)o:(double)(long)o;
            if(tmpValue<minValue)
            {
                minValue = tmpValue;
            }
            if(tmpValue>maxValue)
            {
                maxValue = tmpValue;
            }
        }

        //根据最大最小值分区间
        List<Double> intervalValues = getIntervalArray(minValue, maxValue, ksInterval);

        int intervalNum = intervalValues.size()-1;
        int[] everyIntervalGoodNum = new int[intervalNum];
        int[] everyIntervalBadNum = new int[intervalNum];
        for(int i=0; i<intervalNum; i++) {
            double startValue = intervalValues.get(i);
            double endValue = intervalValues.get(i + 1);
            for (Row row : rows) {
                Object o = row.getAs(featureName);
                double featureValue = o instanceof Long ? (double) (long) o : (double) o;
                long labelValue = row.getAs(labelName);
                if (0 == labelValue && featureValue >= startValue && featureValue < endValue) {
                    everyIntervalGoodNum[i] = everyIntervalGoodNum[i] + 1;
                } else if (1 == labelValue && featureValue >= startValue && featureValue < endValue) {
                    everyIntervalBadNum[i] = everyIntervalBadNum[i] + 1;
                } else {
                }
            }
        }
        //计算累计好坏样本数
        List<Long> accumulateIntervalGoodSampleNums = new ArrayList<>();
        List<Long> accumulateIntervalBadSampleNums = new ArrayList<>();
        for(int i=0; i<ksInterval; i++)
        {
            long goodSum = 0;
            long badSum = 0;
            for(int j=0; j<i+1; j++)
            {
                goodSum = goodSum + everyIntervalGoodNum[j];
                badSum = badSum + everyIntervalBadNum[j];
            }
            accumulateIntervalGoodSampleNums.add(goodSum);
            accumulateIntervalBadSampleNums.add(badSum);
        }

        //根据每个区间的累计好坏样本数占比求出KS值
        double ksValue = 0;
        for(int i=0; i<ksInterval; i++)
        {
            double tmpValue = Math.abs((double)accumulateIntervalGoodSampleNums.get(i)/goodSampleNum-(double)accumulateIntervalBadSampleNums.get(i)/badSampleNum);
            if(tmpValue>ksValue)
                ksValue = tmpValue;
        }
        return ksValue;
    }
    //根据最大最小值和区间计算每个区间的上下限
    public  List<Double> getIntervalArray(double minValue, double maxValue, int intervalNum)
    {
        List<Double> intervalArrayValue = new ArrayList<>();
        intervalArrayValue.add(minValue);
        double intervalValue = (maxValue-minValue)/intervalNum;
        for(int i=0; i<intervalNum; i++)
        {
            intervalArrayValue.add(minValue+intervalValue*(i+1));
        }
        return intervalArrayValue;
    }

    public int getKsInterval() {
        return ksInterval;
    }

    public void setKsInterval(int ksInterval) {
        this.ksInterval = ksInterval;
    }

    public List<Double> getTotalKsValue() {
        return totalKsValue;
    }

    public void setTotalKsValue(List<Double> totalKsValue) {
        this.totalKsValue = totalKsValue;
    }

    public Map<String, Double> getTotalFeaturesImportance() {
        return totalFeaturesImportance;
    }

    public void setTotalFeaturesImportance(Map<String, Double> totalFeaturesImportance) {
        this.totalFeaturesImportance = totalFeaturesImportance;
    }

    public static void main(String[] args)
    {
        KsSelector ksSelector = new KsSelector();
        List<Double> intervalValues = ksSelector.getIntervalArray(0,10, 10);
        SparkSession sparkSession = SparkSession.builder()
                .appName("classification test")
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();
        DataFrameReader dataFrameReader = new DataFrameReader(sparkSession);
        Dataset<Row> irisData = dataFrameReader.table("iris_data_binary");//.where("label=1 or label=2");
        String[] featureNames = {"sepal_length","sepal_width","petal_length","petal_width", "userless"};
        for(int i=0; i<featureNames.length; i++) {
            Dataset<Row> singleFeatureSamples = irisData.select(featureNames[i], "label");
            double ksValue = ksSelector.singleFeatureKsValue(singleFeatureSamples, featureNames[i], "label", 10);
            System.out.println(ksValue);
        }
    }
}
