package com.wacai.stanlee.omega.featureselection;

import com.wacai.stanlee.omega.featurebin.ChiMerge;
import com.wacai.stanlee.omega.util.MapSort;
import org.apache.spark.sql.*;

import java.util.*;

/**
 * @author manshahua@wacai.com
 * @date 2018/1/10 下午6:58
 */
public class IvSelector {
    private int ivInterval = 5;//最终的卡方分箱
    private int startInterval = 80;//等频分箱
    private int minBinNum = 10;
    private Map<String, Double> totalFeaturesImportance = new HashMap<>();
    private Map<String, List<Double>> ivIntervalValues = new HashMap<>();
    private Map<String, IvIntervalStat> ivIntervalStats = new HashMap<>();

    public List<String> featureSelect(FeatureSelectorParam featureSelectorParam) {
        Dataset<Row> samples = featureSelectorParam.getSamples();
        String labelName = featureSelectorParam.getLabelName();
        samples = samples.withColumn(labelName, functions.col(labelName).cast("long"));
        String[] featureColumnNames = featureSelectorParam.getFeatureColumnNames();
        long selectedFeatureNum = featureSelectorParam.getSelectedFeatureNum();
        double selectedFeatureThreshold = featureSelectorParam.getSelectedFeatureThreshold();
        long length = featureColumnNames.length;
        for (int i = 0; i < length; i++) {
            Dataset<Row> singleFeatureSamples = samples.select(featureColumnNames[i], labelName);
            double start = System.currentTimeMillis();
            double ivValue = singleFeatureIvValue2(singleFeatureSamples, featureColumnNames[i], labelName);
            double end = System.currentTimeMillis();
            System.out.println("单个特征选择时间：" + (end - start) / (1000) + "秒！！！！");
            totalFeaturesImportance.put(featureColumnNames[i], ivValue);
        }
        List<String> selectedFeatureNames = new ArrayList<>();
        //根据输入的特征选择数和阈值选出符合条件的特征
        Map<String, Double> sortedFeatureKsValues = MapSort.sortByValueDesc(totalFeaturesImportance);
        if (selectedFeatureNum > 0) {
            int ind = 0;
            for (Map.Entry<String, Double> entry : sortedFeatureKsValues.entrySet()) {
                ind++;
                selectedFeatureNames.add(entry.getKey());
                if (ind >= selectedFeatureNum)
                    break;

            }
        } else {
            for (Map.Entry<String, Double> entry : sortedFeatureKsValues.entrySet()) {
                if (selectedFeatureThreshold > entry.getValue())
                    break;
                selectedFeatureNames.add(entry.getKey());
            }
        }
        return selectedFeatureNames;
    }

    //优化版本空间换取时间
    public List<Double> getIntervalArrayEqualNum2(Dataset<Row> singleFeatureSamples, String featureName, int intervalNum, long totalNum) {
        if (totalNum < intervalNum) {
            System.err.println("样本数比区间数小！！！！！！！！！");
            return null;
        }
        List<Row> rows = singleFeatureSamples.collectAsList();
        //将所有的特征值放入list
        List<Double> featureValues = new ArrayList<>();
        List<Double> totalFeatureValues = new ArrayList<>();
        for (Row row : rows) {
            double value = 0;
            if (row.getAs(featureName) instanceof Double) {
                value = row.getAs(featureName);
            } else {
                value = (double) (long) row.getAs(featureName);
            }
            featureValues.add(value);
        }
        Collections.sort(featureValues);
        //对排序后的所有值进行遍历，将不同的值放入到一个新的list中
        for(int i=0; i<featureValues.size(); i++)
        {
            if(!totalFeatureValues.contains(featureValues.get(i)))
            {
                totalFeatureValues.add(featureValues.get(i));
            }
        }
//        for(int i=0; i<totalFeatureValues.size(); i++)
//        {
//            System.out.println(totalFeatureValues.get(i));
//        }
        if(totalFeatureValues.size()<minBinNum)
            return totalFeatureValues;
        //取出每个rank对应的数值
        int everyIntervalSampleCnt = (int) (totalNum / intervalNum);
//        System.out.println("每个区间样本数："+everyIntervalSampleCnt);
        int valueIndices[] = new int[intervalNum];
        valueIndices[0] = 1;

        for (int i = 1; i < intervalNum - 1; i++) {
            valueIndices[i] = everyIntervalSampleCnt * i;
        }
        valueIndices[intervalNum - 1] = (int) totalNum;
        List<Double> intervalArrayValue = new ArrayList<>();
        for (int i = 0; i < intervalNum; i++) {
            intervalArrayValue.add(featureValues.get(valueIndices[i] - 1));
        }
//        for(int i=0; i<intervalArrayValue.size(); i++)
//        {
//            System.out.print(intervalArrayValue.get(i)+" ");
//        }
//        System.out.println("\n");

        //由于有时候按照等量等分会有某个取值对应的样本量很大的情况，所以只取不同值
        List<Double> intervalArrayValueNew = new ArrayList<>();
        for (int i = 0; i < intervalArrayValue.size(); i++) {
            if (intervalArrayValueNew.contains(intervalArrayValue.get(i))) {
                continue;
            }
            intervalArrayValueNew.add(intervalArrayValue.get(i));
        }
//        for(int i=0; i<intervalArrayValueNew.size(); i++)
//        {
//            System.out.print(intervalArrayValueNew.get(i)+" ");
//        }
//        System.out.println("\n");
        return intervalArrayValueNew;
    }


    public double singleFeatureIvValue2(Dataset<Row> singleFeatureSamples, String featureName, String labelName) {
        List<Row> rows = singleFeatureSamples.collectAsList();

        int rowNum = rows.size();
        //根据等频获得的分箱
        List<Double> intervalValues = getIntervalArrayEqualNum2(singleFeatureSamples, featureName, startInterval, rowNum);
//        System.out.println("初始区间：");
//        for(int i=0; i<intervalValues.size(); i++)
//        {
//            System.out.print(intervalValues.get(i)+" ");
//        }
//        System.out.println("\n");
        int totalGoodNum = 0;
        int totalBadNum = 0;
        //统计总的正负样本
        for (Row row : rows) {
            long labelValue = row.getAs(labelName);
            if (0 == labelValue)
                totalGoodNum = totalGoodNum + 1;
            else if (1 == labelValue)
                totalBadNum = totalBadNum + 1;
        }
        double overAllRate = (double) totalBadNum / (totalBadNum + totalGoodNum);
        //根据卡方分箱法获得的分箱
        ChiMerge chiMerge = new ChiMerge();
//        intervalValues = chiMerge.splitBin(singleFeatureSamples, intervalValues, overAllRate, ivInterval, featureName, labelName);
        //测试一下条件熵函数
//        double conditionEntropy = InformationEntropy.computeIntervalEntropy(singleFeatureSamples,featureName,labelName,intervalValues);
//        System.out.println("条件熵为："+conditionEntropy);
        if (null == intervalValues || intervalValues.size() <= 1)
        {
            System.out.println("区间分隔太少！！！");
            return 0;
        }
        ivIntervalValues.put(featureName, intervalValues);

        int intervalNum = intervalValues.size() - 1;
        int[] everyIntervalGoodNum = new int[intervalNum];
        int[] everyIntervalBadNum = new int[intervalNum];
        //最后那个区间应该是闭区间但是这样取不到，所以取到倒数第二个区间，用总的减去之前的就行
        for (int i = 0; i < intervalNum-1; i++) {
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
                }
            }
        }
        int leftBadTotalNum = totalBadNum;
        int leftGoodTotalNum = totalGoodNum;
        for(int i=0; i<intervalNum-1; i++)
        {
            leftBadTotalNum = leftBadTotalNum - everyIntervalBadNum[i];
            leftGoodTotalNum = leftGoodTotalNum - everyIntervalGoodNum[i];
        }
        everyIntervalBadNum[intervalNum-1] = leftBadTotalNum;
        everyIntervalGoodNum[intervalNum-1] = leftGoodTotalNum;
        //将分隔区间存储起来共分析
        Map<String, String> intervalInfos = new TreeMap<>();
        boolean isMonotone = true;
        //判断单调性
        double[] badRates = new double[intervalNum];
        for (int i = 0; i < intervalNum; i++) {
            double startValue = Math.round(intervalValues.get(i) * 10000) / 10000.00;
            double endValue = Math.round(intervalValues.get(i + 1) * 10000) / 10000.00;
            String key = (i + 1) + "*[" + startValue + "-" + endValue + "]";
            double intervalTotal = everyIntervalBadNum[i] + everyIntervalGoodNum[i];

            double bigi = (double) everyIntervalBadNum[i] / intervalTotal;
            bigi = Math.round(bigi * 10000) / 10000.00;
            badRates[i] = bigi;
            String value = everyIntervalBadNum[i] + "/" + intervalTotal + ":" + bigi;
            intervalInfos.put(key, value);
        }
        for(int i=1; i<intervalNum-1; i++)
        {
            if( (badRates[i]>badRates[i-1]&&badRates[i]>badRates[i+1]) || (badRates[i]<badRates[i-1]&&badRates[i]<badRates[i+1]))
            {
                isMonotone = false;
                break;
            }
        }
        IvIntervalStat ivIntervalStat = new IvIntervalStat(featureName, isMonotone, intervalInfos);

        ivIntervalStats.put(featureName, ivIntervalStat);
        //根据woe  IV计算公式计算值 woe=ln(bi/bt)/(gi/gt) iv=(bi/bt-gi/gt)*woe
        double ivValue = 0;
        for (int i = 0; i < intervalNum; i++) {
            double bibt = (double) everyIntervalBadNum[i] / totalBadNum;
            double gigt = (double) everyIntervalGoodNum[i] / totalGoodNum;
            bibt = bibt == 0 ? 1 : bibt;
            gigt = gigt == 0 ? 1 : gigt;
            double woe = Math.log(gigt / bibt);//Math.log(bibt/gigt);
            double iv = (gigt - bibt) * woe;//(bibt-gigt)*woe;
            ivValue = ivValue + iv;
        }
        System.out.println(featureName + "的iv值为：" + ivValue);
        return ivValue;
    }

    public double singleFeatureIvValue(Dataset<Row> singleFeatureSamples, String featureName, String labelName, int ksInterval) {
        SparkSession sparkSession = singleFeatureSamples.sparkSession();
        List<Row> labels = singleFeatureSamples.select(labelName).distinct().collectAsList();
        //取出label的值 0，1
        if (labels.size() >= 3) {
            System.out.println("Ks selector error the num of classes bigger than 2 ,ks only used for binary classification");
            return 0;
        }
        //好的样本标签为1，坏的样本标签为0
        long goodSampleNum = singleFeatureSamples.where(labelName + "=1").count();
        long badSampleNum = singleFeatureSamples.where(labelName + "=0").count();

        //根据最大最小值分区间
        List<Double> intervals = getIntervalArrayEqualNum2(singleFeatureSamples, featureName, ksInterval, goodSampleNum + badSampleNum);
        if (null == intervals || intervals.size() <= 1)
            return 0;
        ivIntervalValues.put(featureName, intervals);
        int newKsInterval = intervals.size() - 1;
        List<String> whereConditions = new ArrayList<>();
        for (int i = 0; i < newKsInterval; i++) {
            double startValue = intervals.get(i);
            double endValue = intervals.get(i + 1);
            String whereCondition = featureName + ">=" + startValue + " and " + featureName + (i == newKsInterval - 1 ? "<=" : "<") + endValue;
            whereConditions.add(whereCondition);
        }
        //直接通过一个sql搞定 不然太慢了
        String caseWhenSql = "";
        for (int i = 0; i < newKsInterval; i++) {
            caseWhenSql = caseWhenSql +
                    "case when " + whereConditions.get(i) + " and " + labelName + "=1 then 1 else 0 end as " + featureName + "_" + i + labelName + "_1,\n" +
                    "case when " + whereConditions.get(i) + " and " + labelName + "=0 then 1 else 0 end as " + featureName + "_" + i + labelName + "_0" + (i == newKsInterval - 1 ? "\n" : ",\n");
        }
        String sumSql = "";
        for (int i = 0; i < newKsInterval; i++) {
            sumSql = sumSql + "sum(" + featureName + "_" + i + labelName + "_0) as sum_" + featureName + "_" + i + "_" + labelName + "_0,\n" +
                    "sum(" + featureName + "_" + i + labelName + "_1) as sum_" + featureName + "_" + i + "_" + labelName + "_1" + (i == newKsInterval - 1 ? "\n" : ",\n");
        }
        singleFeatureSamples.createOrReplaceTempView("singledata");
        Dataset<Row> everyIntervalSampleNums = sparkSession.sql("select \n" + sumSql + " from \n (\n select \n" + caseWhenSql + "from singledata\n)a");
        everyIntervalSampleNums.show();
        //统计每个区间内的正负样本数
        List<Long> intervalGoodSampleNums = new ArrayList<>();
        List<Long> intervalBadSampleNums = new ArrayList<>();
        Row rowData = everyIntervalSampleNums.first();
        for (int i = 0; i < newKsInterval; i++) {
            Long intervalBadSampleNum = rowData.getAs("sum_" + featureName + "_" + i + "_" + labelName + "_0");
            intervalBadSampleNums.add(intervalBadSampleNum);
            Long intervalGoodSampleNum = rowData.getAs("sum_" + featureName + "_" + i + "_" + labelName + "_1");
            intervalGoodSampleNums.add(intervalGoodSampleNum);
        }
        //根据woe  IV计算公式计算值 woe=ln(bi/bt)/(gi/gt) iv=(bi/bt-gi/gt)*woe
        double ivValue = 0;
        for (int i = 0; i < newKsInterval; i++) {
            double bibt = (double) intervalBadSampleNums.get(i) / badSampleNum;
            double gigt = (double) intervalGoodSampleNums.get(i) / goodSampleNum;
            bibt = bibt == 0 ? 1 : bibt;
            gigt = gigt == 0 ? 1 : gigt;
            double woe = Math.log(gigt / bibt);//Math.log(bibt/gigt);
            double iv = (gigt - bibt) * woe;//(bibt-gigt)*woe;
            ivValue = ivValue + iv;
        }
        System.out.println(featureName + "的iv值为：" + ivValue);
        return ivValue;
    }

    //获取每个分隔区间值，选取不同的分隔方式，等距：
    public List<Double> getIntervalArrayEqualValue(Dataset<Row> singleFeatureSamples, String featureName, int intervalNum, long totalNum) {
        Dataset<Row> minMaxValueDataset = singleFeatureSamples.agg(functions.min(functions.col(featureName)), functions.max(functions.col(featureName)));

        double maxValue = 0d;//maxValueDataset.head().get(0) instanceof Long?(long)maxValueDataset.head().get(0):(double)maxValueDataset.head().get(0);
        double minValue = 0d;//minValueDataset.head().get(0) instanceof Long?(long)minValueDataset.head().get(0):(double)minValueDataset.head().get(0);

        Row headRow = minMaxValueDataset.head();
        minValue = headRow.get(0) instanceof Long ? (long) headRow.get(0) : (double) headRow.get(0);
        maxValue = headRow.get(1) instanceof Long ? (long) headRow.get(1) : (double) headRow.get(1);
        List<Double> intervalArrayValue = new ArrayList<>();
        intervalArrayValue.add(minValue);
        double intervalValue = (maxValue - minValue) / intervalNum;
        for (int i = 0; i < intervalNum; i++) {
            intervalArrayValue.add(minValue + intervalValue * (i + 1));
        }
        return intervalArrayValue;
    }

    public List<Double> getIntervalArrayEqualNum(Dataset<Row> singleFeatureSamples, String featureName, int intervalNum, long totalNum) {
        if (totalNum < intervalNum) {
            System.err.println("样本数比区间数小！！！！！！！！！");
            return null;
        }
        //先对数据按照特征进行排序
        SparkSession sparkSession = singleFeatureSamples.sparkSession();
        int everyIntervalSampleCnt = (int) (totalNum / intervalNum);
        //取出每个rank对应的数值
        String valuesStr = "1,";
        for (int i = 1; i < intervalNum - 1; i++) {
            valuesStr = valuesStr + everyIntervalSampleCnt * i + ",";
        }
        valuesStr = valuesStr + totalNum;
        DataFrameWriter dataFrameWriter = new DataFrameWriter(singleFeatureSamples);
        dataFrameWriter.mode("overwrite").saveAsTable("tmp");
        Dataset<Row> rankedSingleFeatureSamples = sparkSession.sql("select\n*,\nrow_number() over(partition by 1 order by " + featureName + ") as rank\nfrom tmp").
                where("rank in (" + valuesStr + ")").sort("rank");
        List<Row> valueRankRows = rankedSingleFeatureSamples.collectAsList();
        int size = valueRankRows.size();
        List<Double> intervalArrayValue = new ArrayList<Double>();
        for (int i = 0; i < size; i++) {
            Row row = valueRankRows.get(i);
            Object o = row.getAs(featureName);
            if (o instanceof Double) {
                intervalArrayValue.add((double) o);
            } else if (o instanceof Long) {
                intervalArrayValue.add((double) (long) o);
            } else
                return null;
        }
//        System.out.println();
//        sparkSession.sql("drop table if exists tmp");
        //由于有时候按照等量等分会有某个取值对应的样本量很大的情况，所以只取不同值
        List<Double> intervalArrayValueNew = new ArrayList<>();
        for (int i = 0; i < intervalArrayValue.size(); i++) {
            if (intervalArrayValueNew.contains(intervalArrayValue.get(i))) {
                continue;
            }
            intervalArrayValueNew.add(intervalArrayValue.get(i));
        }
        return intervalArrayValueNew;
    }

    public int getIvInterval() {
        return ivInterval;
    }

    public void setIvInterval(int ivInterval) {
        this.ivInterval = ivInterval;
    }

    public Map<String, Double> getTotalFeaturesImportance() {
        return totalFeaturesImportance;
    }

    public void setTotalFeaturesImportance(Map<String, Double> totalFeaturesImportance) {
        this.totalFeaturesImportance = totalFeaturesImportance;
    }

    public Map<String, List<Double>> getIvIntervalValues() {
        return ivIntervalValues;
    }

    public void setIvIntervalValues(Map<String, List<Double>> ivIntervalValues) {
        this.ivIntervalValues = ivIntervalValues;
    }

    public Map<String, IvIntervalStat> getIvIntervalStats() {
        return ivIntervalStats;
    }

    public void setIvIntervalStats(Map<String, IvIntervalStat> ivIntervalStats) {
        this.ivIntervalStats = ivIntervalStats;
    }

    public int getStartInterval() {
        return startInterval;
    }

    public void setStartInterval(int startInterval) {
        this.startInterval = startInterval;
    }

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("bintest")
                .enableHiveSupport()
                .getOrCreate();
//        Dataset<Row> data = sparkSession.read().format("orc").load("hdfs://10.1.169.1:9000/data/dw/raw/modeling_db.db/xf_wht_calls_6m_samples_small");
        Dataset<Row> data = sparkSession.read().option("header", "true").option("inferSchema","true").csv("/Users/duty/document/df_credit_action_0.csv");
        data.write().mode("overwrite").saveAsTable("user_info");
      //  Dataset<Row> data = sparkSession.read().table("xf_wht_calls_6m_samples_final_result");
        data.show(10);
        String[] featureNames = data.columns();
        String featureName = "int_rate_clean";
        String labelName = "y";
        Dataset<Row> singleFeatureSamples = data.select("order_id", "label_13", "due_cnt");
        singleFeatureSamples = singleFeatureSamples.withColumn(labelName, functions.col(labelName).cast("long"));
        singleFeatureSamples = singleFeatureSamples.withColumn(featureName, functions.col(featureName).cast("double"));
        singleFeatureSamples.createOrReplaceTempView("tmp");

        FeatureTable featureTable = new FeatureTable();
        featureTable.setTableName("user_info");

    //FeatureTable.fromFile("featureTable.json");
        featureTable.setTableName("tmp");
        FeatureSelectorParam featureSelectorParam = new FeatureSelectorParam();
        featureSelectorParam.init(featureTable, sparkSession);
        IvSelector ivSelector = new IvSelector();
        ivSelector.featureSelect(featureSelectorParam);
        ivSelector.getIvIntervalStats();
        System.out.println("dd");
    }
}
