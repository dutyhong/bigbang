package com.wacai.stanlee.omega.sample;

import com.wacai.stanlee.omega.util.MapSort;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.*;

/**
 * @author manshahua@wacai.com
 * @date 2018/3/9 下午4:14
 */
public class SmoteSample {
    public static void smote(Dataset<Row> samples, String smoteLabelValue, String labelName, List<String> featureNames)
    {
        Dataset<Row> smoteSamples = samples.where(labelName+"="+smoteLabelValue);
        List<Row> smoteRows = smoteSamples.collectAsList();
        int size = smoteRows.size();
        List<double[]> oneNeighbors = new ArrayList<>();
        for(int i=0; i<size; i++)
        {
            Row row = smoteRows.get(i);
            oneNeighbors = getKNeighbors(row, smoteRows, featureNames, "cos", 20, 15);
        }
        System.out.println("ddd");
    }
    public static List<double[]> getKNeighbors(Row centerRow, List<Row> rows, List<String> featureNames, String distMethod, int K,
                                     int N)
    {
        int size = rows.size();
        Map<Integer, Double> distMap = new HashMap<>();
        //把每个row对应的向量存起来后面做线性插值的时候就不需要再遍历了
        List<double[]> rowsVectors = new ArrayList<>();
        int vectorLength = featureNames.size();
        double[] aVectors = new double[size];
        //只需要一遍就可以了
        for(int i=0; i<vectorLength; i++)
        {
            String featureName = featureNames.get(i);
            if(centerRow.getAs(featureName) instanceof Long )
            {
                aVectors[i] = (double) (long)centerRow.getAs(featureName);
            }else{
                aVectors[i] = (double) centerRow.getAs(featureName);
            }
        }
        //循环
        for(int j=0; j<size; j++)
        {
            Row row = rows.get(j);

            double[] bVectors = new double[size];
            for(int i=0; i<vectorLength; i++)
            {
                String featureName = featureNames.get(i);
                if(centerRow.getAs(featureName) instanceof Long )
                {
                    bVectors[i] = (double) (long)row.getAs(featureName);
                }else{
                    bVectors[i] = (double) row.getAs(featureName);
                }
            }
            rowsVectors.add(bVectors);
            //计算距离 欧式或者余弦距离
            double dist = 0;
            if("cos".equals(distMethod))
            {
                dist = cosDist(aVectors, bVectors);
            }else{
                dist = europeanDist(aVectors, bVectors);
            }
            distMap.put(j, dist);
        }
        Map<Integer, Double> sortedMap = MapSort.sortByValueDesc(distMap);
        //选出K个最近邻的样本
        int k = 0;
        List<Row> neighbors = new ArrayList<>();
        for(Map.Entry<Integer,Double> entry: sortedMap.entrySet())
        {
            k++;
            int index = entry.getKey();
            neighbors.add(rows.get(index));
            if(k>K)
                break;
        }
        //再从K个中选取N个进行线性插值产生出新的样本
        Set<Integer> indices = randomInt(0, K, N);
        //取出N个样本
        List<Row> insertSamples = new ArrayList<>();
        for(int index:indices)
        {
            insertSamples.add(rows.get(index));
        }
        //开始线性插值
        List<double[]> resultRows = new ArrayList<>();
        for(int m=0; m<rowsVectors.size(); m++)
        {
            double[] bVectors = rowsVectors.get(m);
            double[] tmpVectors = new double[vectorLength];
            for(int i=0; i<vectorLength; i++)
            {
                tmpVectors[i] = aVectors[i] + Math.random()*(aVectors[i] - bVectors[i]);
            }
            resultRows.add(tmpVectors);
        }
        return resultRows;

    }
    public static double cosDist(double[] aVectors, double[] bVectors)
    {
        int length = aVectors.length;
        double absA = 0;
        double absB = 0;
        double absAB = 0;
        for(int i=0; i<length; i++)
        {
            absA = absA + aVectors[i]*aVectors[i];
            absB = absB + bVectors[i]*bVectors[i];
            absAB = absAB + aVectors[i]*bVectors[i];
        }
        absA = Math.sqrt(absA);
        absB = Math.sqrt(absB);
        double dist = Math.abs(absAB/(absA*absB));
        return dist;
    }
    public static double europeanDist(double[] aVectors, double[] bVectors)
    {
        int length = aVectors.length;
        double absAB = 0;
        for(int i=0; i<length; i++)
        {
            absAB = absAB + Math.pow(aVectors[i]-bVectors[i], 2);
        }
        double dist = Math.sqrt(absAB);
        return dist;
    }
    public static Set<Integer> randomInt(int start , int end, int length)
    {
        Random random = new Random();
        Set<Integer> set = new HashSet<>();
        while(set.size()<length)
        {
            int tmp = random.nextInt(end-start)+start;
            set.add(tmp);
        }
        return set;
    }
    public static void main(String[] args) {
        Set<Integer> set = randomInt(0, 10, 10);
        for (int index : set)
        {
            System.out.println(index);
        }
        SparkSession sparkSession = SparkSession.builder()
                .appName("test")
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();
        Dataset<Row> data = sparkSession.read().table("iris_train_data");
        data.show();
        List<String> featureNames = Arrays.asList("sepal_length","sepal_width","petal_length","petal_width");

        smote(data,"1", "label", featureNames);
        //测试通过数组创建dataset
        double[] list1 = {1,2,3};
        double[] list2 = {4,5,7};
        Row row =  RowFactory.create(1.0,2.0,3.0);// Double.valueOf(list1[1]), Double.valueOf(list1[2]));
        Row row1 = RowFactory.create(4.0,5.0,7.0);
        List<Row> rows = new ArrayList<>();
        rows.add(row);
        rows.add(row1);
        StructField structField1 = DataTypes.createStructField("col1", DataTypes.DoubleType, true);
        StructField structField2 = DataTypes.createStructField("col2", DataTypes.DoubleType, true);
        StructField structField3 = DataTypes.createStructField("col3", DataTypes.DoubleType, true);
        StructType structType = DataTypes.createStructType(new StructField[]{structField1, structField2, structField3});
        Dataset<Row> dataset = sparkSession.createDataFrame(rows, structType);
        dataset.show();
        List<String> strList = Arrays.asList("a","b", "c");

        Dataset<String> strData  = sparkSession.createDataset(strList, Encoders.STRING());
        strData.show();
        strData.printSchema();
        System.out.println("ddd");
    }
}
